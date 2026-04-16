"""
DialFire Multi-Campaign -> weekly_data.json fetcher
Uses the correct DialFire REST API as confirmed by DialFire support.
group0=date&group1=user is required (date must come first).
"""

import os, json, time, requests
from datetime import datetime, timedelta, timezone

raw_campaigns = os.environ.get("DIALFIRE_CAMPAIGNS", "[]")
try:
    CAMPAIGNS = json.loads(raw_campaigns)
except json.JSONDecodeError as e:
    print(f"ERROR Could not parse DIALFIRE_CAMPAIGNS secret: {e}")
    raise

if not CAMPAIGNS:
    raise ValueError("DIALFIRE_CAMPAIGNS secret is empty.")

print(f"Loaded {len(CAMPAIGNS)} campaigns from secret")

today    = datetime.now(timezone.utc).date()
last_mon = today - timedelta(days=today.weekday() + 7)
last_sun = last_mon + timedelta(days=6)
DATE_FROM = last_mon.strftime("%Y-%m-%d")
DATE_TO   = last_sun.strftime("%Y-%m-%d")

RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

def is_rm(name):
    n = name.lower()
    return any(rm.lower() in n or n in rm.lower() for rm in RM_NAMES)

def fetch_campaign(campaign):
    cid   = campaign["id"]
    token = campaign["token"]
    label = campaign.get("name", cid)
    base  = f"https://api.dialfire.com/api/campaigns/{cid}"

    # Try multiple combinations of template, path type, token param, and date format.
    # group0=date MUST come before group1=user per DialFire docs.
    attempts = [
        ("editsDef_v2", "metadata", "_token_",      {"days": "7"}),
        ("editsDef_v2", "metadata", "_token_",      {"from": DATE_FROM, "to": DATE_TO}),
        ("editsDef_v2", "report",   "access_token", {"from": DATE_FROM, "to": DATE_TO}),
        ("dialerStat",  "report",   "access_token", {"from": DATE_FROM, "to": DATE_TO}),
        ("dialerStat",  "metadata", "_token_",      {"days": "7"}),
    ]

    try:
        for template, path_type, token_key, date_params in attempts:
            params = {
                "asTree":  "true",
                "group0":  "date",
                "group1":  "user",
                "column0": "completed",
                "column1": "success",
                "column2": "workTime",
                token_key: token,
                **date_params,
            }
            if template == "dialerStat":
                params["column0"] = "count"
                params["column1"] = "connects"

            url = f"{base}/reports/{template}/{path_type}/de_DE"
            r = requests.get(url, params=params, timeout=30)
            print(f"  [{label}] {template}/{path_type} ({token_key}) -> HTTP {r.status_code}")

            if r.status_code == 401:
                print(f"  WARNING [{label}] 401 Unauthorized - check token")
                return []
            if r.status_code in (403, 404):
                continue
            if r.status_code != 200:
                print(f"  [{label}] HTTP {r.status_code}: {r.text[:200]}")
                continue

            raw  = r.json()
            rows = extract_rows(raw, label)
            if rows:
                print(f"  OK [{label}] {len(rows)} rows via {template}/{path_type}")
                return rows
            else:
                print(f"  [{label}] {template}/{path_type} returned 0 rows - trying next")

        print(f"  FAIL [{label}] No data from any combination")
        return []

    except Exception as e:
        print(f"  FAIL [{label}] Error: {e}")
        return []

def extract_rows(raw, label):
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        if "data" in raw and isinstance(raw["data"], list):
            return raw["data"]
        if "rows" in raw:
            return raw["rows"]
        if "children" in raw or "key" in raw:
            return flatten_tree(raw)
        print(f"    [{label}] Response keys: {list(raw.keys())}")
        if raw and all(isinstance(v, dict) for v in raw.values()):
            return list(raw.values())
    return []

def flatten_tree(node, depth=0):
    rows = []
    if depth > 5:
        return rows
    children = node.get("children") or node.get("data") or node.get("rows") or []
    if isinstance(children, list):
        for child in children:
            if isinstance(child, dict):
                if child.get("children") or child.get("data"):
                    rows.extend(flatten_tree(child, depth + 1))
                else:
                    rows.append(child)
    return rows

def parse_row(row, campaign_name):
    name = (
        row.get("key") or row.get("user") or row.get("agent_name") or
        row.get("username") or row.get("name", "Unknown")
    )
    if isinstance(name, dict):
        name = name.get("label") or name.get("value") or "Unknown"
    name = str(name).strip()

    def safe_int(*keys):
        for k in keys:
            v = row.get(k)
            if v is not None and v != "":
                try: return int(float(str(v)))
                except: pass
        return 0

    def safe_float(*keys):
        for k in keys:
            v = row.get(k)
            if v is not None and v != "":
                try: return float(str(v))
                except: pass
        return 0.0

    calls     = safe_int("completed", "total_calls", "calls", "count", "connects")
    success   = safe_int("success", "total_success")
    rental    = safe_int("RENTAL_LEAD", "rental_lead", "rental")
    seller    = safe_int("LEAD", "seller_lead", "seller")
    email     = safe_int("GOT_EMAIL", "got_email", "email")
    wt_raw    = safe_float("workTime", "work_time", "worktime", "dial_time")
    work_time = round(wt_raw / 3600, 2) if wt_raw > 1000 else round(wt_raw, 2)

    return {
        "name": name, "calls": calls, "success": success,
        "rental": rental, "seller": seller, "email": email,
        "workTime": work_time, "_campaigns": [campaign_name],
    }

def merge_agents(all_rows):
    merged = {}
    for row in all_rows:
        name = row["name"]
        if not name or name.lower() in ("unknown", "system", ""):
            continue
        if name in merged:
            m = merged[name]
            for k in ("calls", "success", "rental", "seller", "email"):
                m[k] += row[k]
            m["workTime"]   = round(m["workTime"] + row["workTime"], 2)
            m["_campaigns"] = list(set(m["_campaigns"] + row["_campaigns"]))
        else:
            merged[name] = dict(row)
    return list(merged.values())

def div_string(campaigns_list):
    return " / ".join(sorted(set(c for c in campaigns_list if c)))

def main():
    print(f"\n{'='*55}")
    print(f"DialFire Multi-Campaign Fetcher")
    print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Week: {DATE_FROM} to {DATE_TO}")
    print(f"Campaigns: {len(CAMPAIGNS)}")
    print(f"{'='*55}\n")

    all_rows = []
    for i, campaign in enumerate(CAMPAIGNS, 1):
        label = campaign.get("name", campaign["id"])
        print(f"[{i}/{len(CAMPAIGNS)}] {label}")
        rows = fetch_campaign(campaign)
        for row in rows:
            parsed = parse_row(row, label)
            if parsed["calls"] > 0:
                all_rows.append(parsed)
        time.sleep(0.3)

    print(f"\nRaw rows fetched: {len(all_rows)}")
    agents = merge_agents(all_rows)
    print(f"Unique agents: {len(agents)}")

    rm, fancy = [], []
    for a in agents:
        div   = div_string(a["_campaigns"])
        clean = {k: v for k, v in a.items() if k != "_campaigns"}
        if is_rm(a["name"]):
            rm.append(clean)
        else:
            fancy.append({**clean, "div": div})

    print(f"RM: {len(rm)}  |  Fancy Callers: {len(fancy)}")

    output = {
        "week":      DATE_FROM,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rm":        sorted(rm,    key=lambda x: x["calls"], reverse=True),
        "fancy":     sorted(fancy, key=lambda x: x["calls"], reverse=True),
    }

    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")

    with open(os.path.join(data_dir, "weekly_data.json"), "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved -> data/weekly_data.json")

    hist_path = os.path.join(data_dir, "history.json")
    history = {}
    if os.path.exists(hist_path):
        with open(hist_path) as f:
            history = json.load(f)
    history[DATE_FROM] = {
        "generated": output["generated"],
        "rm":        output["rm"],
        "fancy":     output["fancy"],
    }
    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Appended -> data/history.json ({len(history)} weeks stored)")

if __name__ == "__main__":
    main()
