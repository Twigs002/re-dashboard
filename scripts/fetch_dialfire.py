"""
DialFire Campaign Stats -> weekly_data.json fetcher
====================================================
Fetches agent stats from DialFire API using per-campaign tokens.

Leads and email counts come from the Dialfire contacts API.
hs_lead_status mapping:
  seller : LEAD (Seller Lead, On the Market, Wants a Valuation)
  rental : RENTAL_LEAD
  email  : GOT_EMAIL
"""

import os, json, re, time, datetime, pytz
import requests

# -- Config -------------------------------------------------------------------
LOCALE = "en_US"
DAYS_BACK = 7
TIMEZONE = pytz.timezone("Africa/Johannesburg")
API_BASE      = "https://api.dialfire.com"
API_BASE_APP  = "https://app.dialfire.com"

BENCHMARKS = {
    "cph": 45,
    "daily_calls": 315,
    "rm_success_rate": 17,
    "fc_success_rate": 20,
}

RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

# hs_lead_status values
SELLER_STATUSES = {"LEAD"}
RENTAL_STATUSES = {"RENTAL_LEAD"}
EMAIL_STATUSES  = {"GOT_EMAIL"}


# -- Poll helper --------------------------------------------------------------
def fetch_json(url, params, label, tag, timeout=30, max_polls=8, headers=None):
    try:
        r = requests.get(url, params=params, timeout=timeout,
                         headers=headers or {})
        if r.status_code == 202:
            poll_url = None
            try:
                poll_url = r.json().get("url") or r.json().get("statusUrl")
            except Exception:
                pass
            if poll_url:
                for _ in range(max_polls):
                    time.sleep(2)
                    r2 = requests.get(poll_url, timeout=timeout)
                    if r2.status_code == 200:
                        print(f"  [{label}] {tag} -> HTTP 200 (after poll)")
                        try:
                            return r2.json()
                        except Exception:
                            return []
                    if r2.status_code == 403:
                        return None
                    if r2.status_code not in (202, 200):
                        return []
            print(f"  [{label}] {tag} -> timed out")
            return []
        if r.status_code == 200:
            print(f"  [{label}] {tag} -> HTTP 200")
            try:
                return r.json()
            except Exception as e:
                print(f"  [{label}] JSON error: {e} | body={r.text[:200]}")
                return []
        if r.status_code == 403:
            return None
        print(f"  [{label}] {tag} -> HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  [{label}] {tag} -> error: {e}")
        return []


# -- Column name extractor ----------------------------------------------------
def _col_names(col_defs):
    if not col_defs:
        return []
    names = []
    for cd in col_defs:
        if isinstance(cd, str):
            names.append(cd)
        elif isinstance(cd, dict):
            names.append(cd.get("name") or cd.get("id") or cd.get("key") or "")
        else:
            names.append("")
    return names


# -- Row extractor ------------------------------------------------------------
def extract_rows(data, label):
    if not isinstance(data, dict):
        return []

    col_defs   = data.get("columnDefs", [])
    grp_names  = _col_names(col_defs)
    groups_raw = data.get("groups", [])

    print(f"  [{label}] DIAG grpDefs={grp_names} groups={type(groups_raw).__name__}[{len(groups_raw) if hasattr(groups_raw,'__len__') else '?'}]")

    def _cn(cd):
        return _col_names(cd) or grp_names

    def _parse_group_item(item, cn):
        if not isinstance(item, dict):
            return None
        if "value" in item and "columns" in item:
            cols = item["columns"]
            name = str(item["value"])
            d = {"name": name}
            if isinstance(cols, list):
                for i, v in enumerate(cols):
                    key = cn[i] if i < len(cn) else f"col{i}"
                    d[key] = v
            elif isinstance(cols, dict):
                d.update(cols)
            return d
        if any(k in item for k in ("user", "name", "username", "agent")):
            name = (item.get("user") or item.get("name") or
                    item.get("username") or item.get("agent") or "")
            d = {"name": str(name)}
            for k in ("completed", "success", "successRate", "workTime"):
                if k in item:
                    d[k] = item[k]
            return d
        return None

    rows = []
    if isinstance(groups_raw, list):
        for item in groups_raw:
            row = _parse_group_item(item, grp_names)
            if row:
                rows.append(row)
    elif isinstance(groups_raw, dict):
        inner_col_defs = groups_raw.get("columnDefs", [])
        cn = _cn(inner_col_defs)
        inner_groups = groups_raw.get("groups", [])
        if isinstance(inner_groups, list):
            for item in inner_groups:
                row = _parse_group_item(item, cn)
                if row:
                    rows.append(row)

    print(f"  [{label}] extracted {len(rows)} rows")
    if rows:
        print(f"  [{label}] sample row: {rows[0]}")
    return rows


# -- Fetch lead/email counts from contacts list API ---------------------------
def fetch_lead_counts(cid, token, period_start, period_end, label):
    """
    Fetch contacts for the campaign and count hs_lead_status per agent.
    Tries multiple endpoint approaches. Returns:
      {agent_name: {"seller": N, "rental": N, "email": N}}
    """
    result = {}
    date_from = str(period_start)
    date_to   = str(period_end)
    ts        = f"{DAYS_BACK}-0day"

    # ---------------------------------------------------------------------------
    # Approach A: Contact list via api.dialfire.com with access_token param
    # GET /api/campaigns/{cid}/contacts?access_token=...&from=YYYY-MM-DD&to=YYYY-MM-DD
    # ---------------------------------------------------------------------------
    approaches = [
        # A: api.dialfire.com contacts list with date params
        {
            "url": f"{API_BASE}/api/campaigns/{cid}/contacts",
            "params": {
                "access_token": token,
                "from": date_from,
                "to": date_to,
                "limit": 1000,
            },
            "headers": None,
            "tag": "api/contacts?from=to",
        },
        # B: api.dialfire.com contacts with timespan
        {
            "url": f"{API_BASE}/api/campaigns/{cid}/contacts",
            "params": {
                "access_token": token,
                "timespan": ts,
                "limit": 1000,
            },
            "headers": None,
            "tag": "api/contacts?timespan",
        },
        # C: app.dialfire.com contacts with Bearer auth
        {
            "url": f"{API_BASE_APP}/api/campaigns/{cid}/contacts",
            "params": {
                "from": date_from,
                "to": date_to,
                "limit": 1000,
            },
            "headers": {"Authorization": f"Bearer {token}"},
            "tag": "app/contacts?from=to Bearer",
        },
        # D: api.dialfire.com firebase contacts report with access_token
        {
            "url": f"{API_BASE}/api/campaigns/{cid}/firebase/reports/contacts",
            "params": {
                "access_token": token,
                "from": date_from,
                "to": date_to,
                "groupBy": "agent",
            },
            "headers": None,
            "tag": "api/firebase/reports/contacts",
        },
        # E: app.dialfire.com firebase reports contacts with Bearer
        {
            "url": f"{API_BASE_APP}/api/campaigns/{cid}/firebase/reports/contacts",
            "params": {
                "from": date_from,
                "to": date_to,
                "groupBy": "agent",
            },
            "headers": {"Authorization": f"Bearer {token}"},
            "tag": "app/firebase/reports/contacts Bearer",
        },
        # F: editsDef_v2 with timespan grouped by user only (get columns)
        # Then parse individual contact fields from the extended data
        {
            "url": f"{API_BASE}/api/campaigns/{cid}/reports/editsDef_v2/report/{LOCALE}",
            "params": {
                "access_token": token,
                "asTree": "true",
                "timespan": ts,
                "group0": "user",
                "column0": "completed",
                "column1": "success",
                "column2": "successRate",
                "column3": "workTime",
                "column4": "hs_lead_status",
            },
            "headers": None,
            "tag": "editsDef_v2/user with hs_lead_status column",
        },
    ]

    for ap in approaches:
        data = fetch_json(ap["url"], ap["params"], label, ap["tag"],
                          headers=ap["headers"])
        print(f"  [{label}] lead-counts via {ap['tag']}: type={type(data).__name__}")

        if data is None:
            print(f"  [{label}]   -> 403")
            continue

        # Handle list response (contacts list)
        if isinstance(data, list) and len(data) > 0:
            print(f"  [{label}]   -> list with {len(data)} items - checking structure")
            # Check if items have hs_lead_status and user/agent fields
            sample = data[0] if data else {}
            print(f"  [{label}]   sample keys: {list(sample.keys())[:10]}")

            has_status = any("hs_lead_status" in str(item) for item in data[:5])
            if has_status:
                print(f"  [{label}]   -> has hs_lead_status! parsing...")
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    status = str(item.get("hs_lead_status", "")).strip().upper()
                    agent  = str(item.get("user") or item.get("agent") or
                                item.get("username") or item.get("name") or "").strip()
                    if not agent:
                        continue
                    if agent not in result:
                        result[agent] = {"seller": 0, "rental": 0, "email": 0}
                    if status in SELLER_STATUSES:
                        result[agent]["seller"] += 1
                    elif status in RENTAL_STATUSES:
                        result[agent]["rental"] += 1
                    elif status in EMAIL_STATUSES:
                        result[agent]["email"] += 1

                if result:
                    print(f"  [{label}]   -> SUCCESS! counts: {result}")
                    return result
            else:
                print(f"  [{label}]   -> no hs_lead_status in list response")
            continue

        # Handle dict response (grouped report)
        if not isinstance(data, dict):
            continue

        groups = data.get("groups", [])
        if not isinstance(groups, list) or len(groups) == 0:
            print(f"  [{label}]   -> empty groups")
            continue

        print(f"  [{label}]   -> {len(groups)} groups - checking for nested status groups")

        for agent_item in groups:
            if not isinstance(agent_item, dict):
                continue
            agent_name = str(agent_item.get("value", agent_item.get("name", ""))).strip()
            if not agent_name:
                continue
            if agent_name not in result:
                result[agent_name] = {"seller": 0, "rental": 0, "email": 0}

            inner = agent_item.get("groups", agent_item.get("children", []))
            if not isinstance(inner, list):
                continue

            for status_item in inner:
                if not isinstance(status_item, dict):
                    continue
                status_val = str(status_item.get("value", status_item.get("name", ""))).strip().upper()
                cols = status_item.get("columns", [])
                count = int(cols[0]) if (isinstance(cols, list) and len(cols) > 0) else 1
                if status_val in SELLER_STATUSES:
                    result[agent_name]["seller"] += count
                elif status_val in RENTAL_STATUSES:
                    result[agent_name]["rental"] += count
                elif status_val in EMAIL_STATUSES:
                    result[agent_name]["email"] += count

        if result:
            print(f"  [{label}]   -> SUCCESS with grouped data: {result}")
            return result

    print(f"  [{label}] All lead-count approaches failed - leads will be 0")
    return result


# -- Parse one row into agent dict -------------------------------------------
def parse_row(row):
    if not isinstance(row, dict):
        return None
    name = str(row.get("name", "")).strip()
    if not name or name in ("", "\u2014", "\u2013"):
        return None

    def _int(v):
        try:
            return int(round(float(v or 0)))
        except Exception:
            return 0

    def _float(v):
        try:
            return round(float(v or 0), 2)
        except Exception:
            return 0.0

    calls   = _int(row.get("completed") or row.get("calls") or 0)
    success = _int(row.get("success", 0))
    wt_raw  = row.get("workTime") or row.get("work_time") or row.get("workHours") or 0
    work_h  = _float(wt_raw)
    # workTime from editsDef_v2 is in hours already (e.g. 0.025 = 1.5 min)
    # Only convert if it looks like seconds (> 1000)
    if work_h > 1000:
        work_h = round(work_h / 3600, 2)

    sr_raw  = row.get("successRate") or row.get("success_rate") or 0
    try:
        sr = round(float(sr_raw), 1)
    except Exception:
        sr = round(success / calls * 100, 1) if calls else 0.0

    seller = _int(row.get("seller", 0))
    rental = _int(row.get("rental", 0))
    email  = _int(row.get("email", 0))

    cph_val = round(calls / work_h, 1) if work_h > 0 else 0.0
    is_rm   = name in RM_NAMES
    bench   = BENCHMARKS["rm_success_rate"] if is_rm else BENCHMARKS["fc_success_rate"]
    meets   = cph_val >= BENCHMARKS["cph"] and sr >= bench

    return {
        "name":        name,
        "calls":       calls,
        "success":     success,
        "seller":      seller,
        "rental":      rental,
        "email":       email,
        "cph":         cph_val,
        "successRate": sr,
        "workTime":    work_h,
        "meetsTarget": meets,
    }


# -- Fetch one campaign -------------------------------------------------------
def fetch_campaign(cid, token, index, total, period_start, period_end):
    label = f"{index + 1}/{total} {cid}"
    base = f"{API_BASE}/api/campaigns/{cid}"

    timespans = ["0-0day", f"{DAYS_BACK}-0day", "14-0day", "7-0day", "30-0day"]

    for ts in timespans:
        params = {
            "access_token": token,
            "asTree": "true",
            "timespan": ts,
            "group0": "user",
            "column0": "completed",
            "column1": "success",
            "column2": "successRate",
            "column3": "workTime",
        }
        data = fetch_json(f"{base}/reports/editsDef_v2/report/{LOCALE}", params,
                          label, f"editsDef_v2 ts={ts}")
        if data is None:
            print(f"  [{label}] 403 - token invalid, skipping campaign")
            return []
        if isinstance(data, dict):
            grp = data.get("groups", [])
            grp_len = len(grp) if hasattr(grp, "__len__") else 0
            print(f"  [{label}] ts={ts} groups={type(grp).__name__}[{grp_len}]")
            if grp_len > 0:
                rows = extract_rows(data, label)
                if rows:
                    print(f"  [{label}] SUCCESS with ts={ts}")
                    lead_counts = fetch_lead_counts(cid, token, period_start, period_end, label)
                    for row in rows:
                        name = row.get("name", "")
                        if name in lead_counts:
                            row["seller"] = lead_counts[name]["seller"]
                            row["rental"] = lead_counts[name]["rental"]
                            row["email"]  = lead_counts[name]["email"]
                    return rows
        else:
            print(f"  [{label}] ts={ts} got non-dict: {type(data).__name__}")

    print(f"  [{label}] all timespans failed")
    return []


# -- Main ---------------------------------------------------------------------
def main():
    now_utc    = datetime.datetime.now(datetime.timezone.utc)
    now_sast   = now_utc.astimezone(TIMEZONE)
    period_end   = now_sast.date()
    period_start = period_end - datetime.timedelta(days=DAYS_BACK)

    print("=== DialFire Weekly Fetch ===")
    print(f"Period : {period_start} to {period_end} ({DAYS_BACK} days)")

    campaigns = []

    ch_id  = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
    ch_tok = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()
    if ch_id and ch_tok:
        campaigns.append({"id": ch_id, "token": ch_tok, "label": "CLIENTHUB"})
        print(f"  CLIENTHUB campaign: {ch_id}")
    elif ch_id:
        print(f"  CLIENTHUB campaign: {ch_id} (NO TOKEN)")

    i = 1
    while True:
        cid  = os.environ.get(f"CAMPAIGN_{i}_ID", "").strip()
        ctok = os.environ.get(f"CAMPAIGN_{i}_TOKEN", "").strip()
        if not cid:
            break
        if ctok:
            campaigns.append({"id": cid, "token": ctok, "label": f"CAMP{i}"})
            print(f"  Campaign {i}: {cid}")
        else:
            print(f"  Campaign {i}: {cid} (NO TOKEN)")
        i += 1

    if not campaigns:
        print("No campaigns configured.")
        return

    print(f"Total campaigns: {len(campaigns)}")
    print()

    all_rows = []
    for idx, c in enumerate(campaigns):
        rows = fetch_campaign(c["id"], c["token"], idx, len(campaigns), period_start, period_end)
        all_rows.extend(rows)

    print()
    print(f"Raw rows collected: {len(all_rows)}")

    merged = {}
    for row in all_rows:
        agent = parse_row(row)
        if agent is None:
            continue
        name = agent["name"]
        if name in merged:
            ex = merged[name]
            ex["calls"]    += agent["calls"]
            ex["success"]  += agent["success"]
            ex["seller"]   += agent["seller"]
            ex["rental"]   += agent["rental"]
            ex["email"]    += agent["email"]
            ex["workTime"]  = round(ex["workTime"] + agent["workTime"], 2)
        else:
            merged[name] = agent

    agents = list(merged.values())
    for a in agents:
        a["cph"] = round(a["calls"] / a["workTime"], 1) if a["workTime"] > 0 else 0.0
        is_rm    = a["name"] in RM_NAMES
        bench    = BENCHMARKS["rm_success_rate"] if is_rm else BENCHMARKS["fc_success_rate"]
        a["meetsTarget"] = a["cph"] >= BENCHMARKS["cph"] and a["successRate"] >= bench

    rm_agents    = sorted([a for a in agents if a["name"] in RM_NAMES],    key=lambda x: -x["calls"])
    fancy_agents = sorted([a for a in agents if a["name"] not in RM_NAMES], key=lambda x: -x["calls"])

    print(f"Unique agents: {len(agents)}")
    print(f"RM: {len(rm_agents)} | Fancy: {len(fancy_agents)}")
    for a in rm_agents:
        print(f"  RM   {a['name']:<22} calls={a['calls']:>4} success={a['success']:>3} "
              f"seller={a['seller']:>3} rental={a['rental']:>3} email={a['email']:>3} cph={a['cph']:>5}")
    for a in fancy_agents:
        print(f"  FANCY {a['name']:<22} calls={a['calls']:>4} success={a['success']:>3} "
              f"seller={a['seller']:>3} rental={a['rental']:>3} email={a['email']:>3} cph={a['cph']:>5}")

    week_str = str(period_start)
    output = {
        "generated":   now_utc.isoformat(),
        "week":        week_str,
        "periodStart": str(period_start),
        "periodEnd":   str(period_end),
        "rm":          rm_agents,
        "fancy":       fancy_agents,
    }

    out_path = os.path.join(os.path.dirname(__file__), "..", "data", "weekly_data.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nWritten to {out_path}")

    hist_path = os.path.join(os.path.dirname(__file__), "..", "data", "history.json")
    history = []
    if os.path.exists(hist_path):
        with open(hist_path) as f:
            try:
                history = json.load(f)
            except Exception:
                history = []

    if not isinstance(history, list):
        history = []

    history = [e for e in history if e.get("weekStart") != str(period_start) and
               e.get("week") != week_str and e.get("periodStart") != str(period_start)]
    history.insert(0, {
        "generated": now_utc.isoformat(),
        "week":      week_str,
        "weekStart": str(period_start),
        "weekEnd":   str(period_end),
        "rm":        rm_agents,
        "fancy":     fancy_agents,
    })

    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"History updated: {len(history)} weeks")


if __name__ == "__main__":
    main()
