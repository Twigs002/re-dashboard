"""
DialFire Multi-Campaign -> weekly_data.json fetcher
=====================================================
Uses tenant API to discover all campaigns, then fetches
per-campaign reports.

Secrets required:
  DIALFIRE_TENANT_ID    - e.g. 3f88c548
  DIALFIRE_TENANT_TOKEN - tenant-level Bearer token
"""

import os, json, time, requests
from datetime import datetime, timedelta, timezone

TENANT_ID    = os.environ.get("DIALFIRE_TENANT_ID", "").strip()
TENANT_TOKEN = os.environ.get("DIALFIRE_TENANT_TOKEN", "").strip()

if not TENANT_ID or not TENANT_TOKEN:
    raise ValueError(
        "DIALFIRE_TENANT_ID and DIALFIRE_TENANT_TOKEN secrets must be set.\n"
        "  DIALFIRE_TENANT_ID    = 3f88c548\n"
        "  DIALFIRE_TENANT_TOKEN = your tenant token"
    )

# ── Date range: last full Mon–Sun week ────────────────────────────
today    = datetime.now(timezone.utc).date()
last_mon = today - timedelta(days=today.weekday() + 7)
last_sun = last_mon + timedelta(days=6)
DATE_FROM = last_mon.strftime("%Y-%m-%d")
DATE_TO   = last_sun.strftime("%Y-%m-%d")

# days_back: how many days to cover (covers last full week + buffer)
DAYS_BACK = (today - last_mon).days + 1   # e.g. 8

# ── Classify RM vs Fancy Caller ───────────────────────────────────
RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

def is_rm(name):
    n = name.lower()
    return any(rm.lower() in n or n in rm.lower() for rm in RM_NAMES)

# ── Step 1: Discover all campaigns via tenant API ─────────────────
def get_all_campaigns():
    url = f"https://api.dialfire.com/api/tenants/{TENANT_ID}/campaigns/"
    print(f"Fetching campaign list from tenant API...")
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {TENANT_TOKEN}"},
            timeout=30
        )
        print(f"  Tenant API -> HTTP {r.status_code}")
        if r.status_code != 200:
            print(f"  ERROR: {r.text[:300]}")
            return []
        data = r.json()
        campaigns = data if isinstance(data, list) else data.get("campaigns", [])
        print(f"  Found {len(campaigns)} campaigns total")
        return campaigns
    except Exception as e:
        print(f"  FAIL tenant API: {e}")
        return []

# ── Step 2: Fetch report for one campaign ─────────────────────────
def fetch_report(campaign):
    cid   = campaign.get("id", "")
    label = campaign.get("title") or campaign.get("name") or cid
    token = campaign.get("permissions", {}).get("token", "")

    if not token:
        return []

    base = f"https://api.dialfire.com/api/campaigns/{cid}"

    # ── Attempt order (most likely to work first) ─────────────────
    #
    # Key findings from DialFire support:
    #   1. /report/ uses: access_token=, timespan=0-Nday, LOCALE MUST HAVE TIMEZONE
    #      e.g. dialerStat/report/de_DE/Africa/Johannesburg?access_token=X&timespan=0-30day
    #
    #   2. /metadata/ uses: _token_=, days=N
    #      e.g. editsDef_v2/metadata/de_DE?_token_=X&days=30
    #
    #   3. group0/group1 params APPEAR to cause 500 on /report/ — try WITHOUT first
    #
    # We try:
    #   A) /report/ with timespan, NO group params (exactly as support example)
    #   B) /report/ with timespan + group0=user (just user, no date)
    #   C) /report/ with timespan + group0=date + group1=user + asTree
    #   D) /metadata/ with days, group0=user (user-only grouping)
    #   E) /metadata/ with days, group0=date + group1=user + asTree

    rows = []

    # ── A: dialerStat/report — MINIMAL params, no grouping ────────
    for template in ("dialerStat", "editsDef_v2", "activities"):
        url  = f"{base}/reports/{template}/report/de_DE/Africa/Johannesburg"
        params = {
            "access_token": token,
            "timespan":     f"0-{DAYS_BACK}day",
        }
        rows = _try_fetch(url, params, label, f"{template}/report[minimal]")
        if rows:
            return rows

    # ── B: dialerStat/report — group0=user (no date nesting) ──────
    for template in ("dialerStat", "editsDef_v2"):
        url  = f"{base}/reports/{template}/report/de_DE/Africa/Johannesburg"
        params = {
            "access_token": token,
            "timespan":     f"0-{DAYS_BACK}day",
            "group0":       "user",
        }
        rows = _try_fetch(url, params, label, f"{template}/report[group=user]")
        if rows:
            return rows

    # ── C: dialerStat/report — full tree (date+user) ──────────────
    for template in ("dialerStat", "editsDef_v2"):
        url  = f"{base}/reports/{template}/report/de_DE/Africa/Johannesburg"
        params = {
            "access_token": token,
            "timespan":     f"0-{DAYS_BACK}day",
            "asTree":       "true",
            "group0":       "date",
            "group1":       "user",
        }
        rows = _try_fetch(url, params, label, f"{template}/report[tree]")
        if rows:
            return rows

    # ── D: editsDef_v2/metadata — group0=user (no date nesting) ───
    for template in ("editsDef_v2", "dialerStat", "activities"):
        url  = f"{base}/reports/{template}/metadata/de_DE"
        params = {
            "_token_": token,
            "days":    str(DAYS_BACK),
            "group0":  "user",
        }
        rows = _try_fetch(url, params, label, f"{template}/metadata[group=user]")
        if rows:
            return rows

    # ── E: editsDef_v2/metadata — full tree (date+user) ───────────
    for template in ("editsDef_v2", "dialerStat"):
        url  = f"{base}/reports/{template}/metadata/de_DE"
        params = {
            "_token_": token,
            "days":    str(DAYS_BACK),
            "asTree":  "true",
            "group0":  "date",
            "group1":  "user",
        }
        rows = _try_fetch(url, params, label, f"{template}/metadata[tree]")
        if rows:
            return rows

    # ── F: /metadata/ with from/to date range ─────────────────────
    for template in ("editsDef_v2", "dialerStat"):
        url  = f"{base}/reports/{template}/metadata/de_DE"
        params = {
            "_token_": token,
            "from":    DATE_FROM,
            "to":      DATE_TO,
            "group0":  "user",
        }
        rows = _try_fetch(url, params, label, f"{template}/metadata[from/to]")
        if rows:
            return rows

    print(f"  FAIL [{label}] No data from any combination")
    return []


def _try_fetch(url, params, label, tag):
    """Make a single request and return rows if successful, else []."""
    try:
        r = requests.get(url, params=params, timeout=30)
        status_line = f"  [{label}] {tag} -> HTTP {r.status_code}"

        if r.status_code == 401:
            print(f"{status_line}  (bad token)")
            return None   # stop trying for this campaign
        if r.status_code == 403:
            print(f"{status_line}  (forbidden)")
            return []
        if r.status_code == 404:
            print(f"{status_line}  (not found)")
            return []
        if r.status_code != 200:
            snippet = r.text[:120].replace("\n", " ")
            print(f"{status_line}  {snippet}")
            return []

        # HTTP 200 — parse
        try:
            raw = r.json()
        except Exception:
            print(f"{status_line}  (not JSON)")
            return []

        rows = extract_rows(raw, label, tag)
        if rows:
            print(f"{status_line}  -> {len(rows)} rows  ✓")
        else:
            print(f"{status_line}  -> 0 rows")
        return rows

    except requests.RequestException as e:
        print(f"  [{label}] {tag} -> network error: {e}")
        return []


def extract_rows(raw, label, tag=""):
    """Parse any DialFire response shape into a flat list of dicts."""
    # Print a one-line summary of what we received
    if isinstance(raw, dict):
        keys = list(raw.keys())[:8]
        grp  = raw.get("groups")
        grp_len = len(grp) if isinstance(grp, list) else ("dict" if isinstance(grp, dict) else type(grp).__name__ if grp is not None else "missing")
        print(f"    [{label}] keys={keys}  groups={grp_len}")

    # List response
    if isinstance(raw, list):
        rows = flatten_groups(raw)
        return rows

    if not isinstance(raw, dict):
        return []

    # "groups" key — nested tree
    if "groups" in raw:
        g = raw["groups"]
        if isinstance(g, list) and g:
            first_keys = list(g[0].keys()) if isinstance(g[0], dict) else []
            print(f"    [{label}] first group keys: {first_keys}")
        rows = flatten_groups(g if isinstance(g, list) else [])
        return rows

    # Flat "data" or "rows" arrays
    for key in ("data", "rows", "records", "items", "result"):
        if key in raw and isinstance(raw[key], list):
            return raw[key]

    # If there are numeric values at the top level with a "key" field — single row
    if "key" in raw:
        return [raw]

    return []


def flatten_groups(groups, depth=0):
    """
    Recursively flatten DialFire groups tree into agent rows.
    Each node: { key: str, values: {col: val}, groups: [...] }
    """
    rows = []
    if depth > 6 or not isinstance(groups, list):
        return rows
    for node in groups:
        if not isinstance(node, dict):
            continue
        values     = node.get("values") or {}
        sub_groups = node.get("groups")
        if sub_groups and isinstance(sub_groups, list) and sub_groups:
            rows.extend(flatten_groups(sub_groups, depth + 1))
        else:
            # Leaf node — this is an agent row
            key = node.get("key", "")
            row = {"name": key}
            if isinstance(values, dict):
                row.update(values)
            rows.append(row)
    return rows


# ── Parse one row into our standard schema ────────────────────────
def parse_row(row, campaign_name):
    name = (
        row.get("name") or row.get("key") or row.get("user") or
        row.get("agent_name") or row.get("username") or "Unknown"
    )
    if isinstance(name, dict):
        name = name.get("label") or name.get("value") or "Unknown"
    name = str(name).strip()

    def safe_int(*keys):
        for k in keys:
            v = row.get(k)
            if v is not None and v != "":
                try:
                    return int(float(str(v)))
                except (ValueError, TypeError):
                    pass
        return 0

    def safe_float(*keys):
        for k in keys:
            v = row.get(k)
            if v is not None and v != "":
                try:
                    return float(str(v))
                except (ValueError, TypeError):
                    pass
        return 0.0

    calls   = safe_int("completed", "total_calls", "calls", "count", "connects")
    success = safe_int("success", "total_success")
    rental  = safe_int("RENTAL_LEAD", "rental_lead", "rental")
    seller  = safe_int("SELLER_LEAD", "seller_lead", "seller")
    email   = safe_int("GOT_EMAIL", "got_email", "email")

    wt_raw    = safe_float("workTime", "work_time", "worktime", "dial_time")
    work_time = round(wt_raw / 3600, 2) if wt_raw > 1000 else round(wt_raw, 2)

    return {
        "name":       name,
        "calls":      calls,
        "success":    success,
        "rental":     rental,
        "seller":     seller,
        "email":      email,
        "workTime":   work_time,
        "_campaigns": [campaign_name],
    }


# ── Merge agents across campaigns ─────────────────────────────────
def merge_agents(all_rows):
    merged = {}
    for row in all_rows:
        name = row["name"]
        if not name or name.lower() in ("unknown", "system", ""):
            continue
        if name in merged:
            m = merged[name]
            m["calls"]     += row["calls"]
            m["success"]   += row["success"]
            m["rental"]    += row["rental"]
            m["seller"]    += row["seller"]
            m["email"]     += row["email"]
            m["workTime"]   = round(m["workTime"] + row["workTime"], 2)
            m["_campaigns"] = list(set(m["_campaigns"] + row["_campaigns"]))
        else:
            merged[name] = dict(row)
    return list(merged.values())


def div_string(campaigns_list):
    return " / ".join(sorted(set(c for c in campaigns_list if c)))


# ── Main ──────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"DialFire Multi-Campaign Fetcher  (tenant API)")
    print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Week: {DATE_FROM} to {DATE_TO}  (days_back={DAYS_BACK})")
    print(f"Tenant ID: {TENANT_ID}")
    print(f"{'='*60}\n")

    campaigns = get_all_campaigns()
    if not campaigns:
        raise RuntimeError("No campaigns found from tenant API — check DIALFIRE_TENANT_ID and DIALFIRE_TENANT_TOKEN")

    # Only process active / non-hidden campaigns
    active = [c for c in campaigns if not c.get("hidden", False)]
    print(f"\nProcessing {len(active)} active campaigns (of {len(campaigns)} total)\n")

    all_rows = []
    for i, campaign in enumerate(active, 1):
        label = campaign.get("title") or campaign.get("name") or campaign.get("id", "?")
        cid   = campaign.get("id", "")
        token = campaign.get("permissions", {}).get("token", "")
        if not token:
            print(f"[{i}/{len(active)}] SKIP {label} (no token)")
            continue

        print(f"[{i}/{len(active)}] {label}  ({cid})")
        rows = fetch_report(campaign)
        for row in rows:
            parsed = parse_row(row, label)
            if parsed["calls"] > 0:
                all_rows.append(parsed)
        time.sleep(0.2)

    print(f"\n{'─'*50}")
    print(f"Raw agent rows with calls > 0: {len(all_rows)}")

    agents = merge_agents(all_rows)
    print(f"Unique agents after merge:     {len(agents)}")

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
    os.makedirs(data_dir, exist_ok=True)

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
