"""
DialFire Campaign Stats -> weekly_data.json fetcher
====================================================
Fetches agent stats from DialFire API using per-campaign tokens.

Based on testing: Dialfire timespan format is "0-Nday" = today + N days future (wrong for us)
                  Use -N-0day for last N days (past)
                  Use 0-0day for today only
"""

import os, json, re, time, datetime, pytz
import requests

# -- Config -------------------------------------------------------------------
LOCALE    = "en_US"
DAYS_BACK = 7   # look at the past 7 days (Mon-Sun)
TIMEZONE  = pytz.timezone("Africa/Johannesburg")
API_BASE  = "https://api.dialfire.com"

BENCHMARKS = {
    "cph":            45,
    "daily_calls":    315,
    "rm_success_rate":  17,
    "fc_success_rate":  20,
}

RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}


# -- Poll helper --------------------------------------------------------------
def fetch_json_report(url, params, label, tag, max_polls=8, poll_interval=3):
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 403:
            print(f"  [{label}] {tag} -> HTTP 403  (skip)")
            return None
        if r.status_code == 404:
            print(f"  [{label}] {tag} -> HTTP 404  (skip)")
            return None
        if r.status_code == 202:
            print(f"  [{label}] {tag} -> HTTP 202  (async, polling...)")
            for attempt in range(max_polls):
                time.sleep(poll_interval)
                r2 = requests.get(url, params=params, timeout=30)
                if r2.status_code == 200:
                    print(f"  [{label}] {tag} -> HTTP 200 (after poll)")
                    try:
                        return r2.json()
                    except Exception as e:
                        print(f"  [{label}] JSON parse error after poll: {e}")
                        return []
                if r2.status_code == 403:
                    return None
                if r2.status_code not in (202, 200):
                    print(f"  [{label}] {tag} -> HTTP {r2.status_code} during poll")
                    return []
            print(f"  [{label}] {tag} -> timed out after {max_polls} polls")
            return []
        if r.status_code == 200:
            print(f"  [{label}] {tag} -> HTTP 200")
            try:
                return r.json()
            except Exception as e:
                print(f"  [{label}] JSON parse error: {e} | body={r.text[:200]}")
                return []
        print(f"  [{label}] {tag} -> HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  [{label}] {tag} -> Exception: {e}")
        return []


# -- Column helpers -----------------------------------------------------------
def _safe_int(v):
    try:
        return int(float(v)) if v not in (None, "", "--") else 0
    except (TypeError, ValueError):
        return 0


def _safe_float(v, default=0.0):
    try:
        return float(v) if v not in (None, "", "--") else default
    except (TypeError, ValueError):
        return default


def _get_col(stats, col_map, *names, default=0):
    for name in names:
        mapped = col_map.get(name)
        if mapped and mapped in stats:
            return stats[mapped]
        if name in stats:
            return stats[name]
    return default


def _build_col_names(col_defs_raw):
    result = []
    for item in col_defs_raw:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            result.append(item.get("name", ""))
        else:
            result.append(str(item))
    return result


# -- Parse asTree=true JSON into user rows ------------------------------------
def extract_rows_from_tree(data, label):
    rows = []
    if not isinstance(data, dict):
        print(f"  [{label}] DIAG: data is not dict, type={type(data).__name__}")
        return rows

    col_defs_raw   = data.get("columnDefs", [])
    group_defs_raw = data.get("groupDefs", [])
    groups_raw     = data.get("groups", data.get("children", {}))

    col_names   = _build_col_names(col_defs_raw)
    group_names = _build_col_names(group_defs_raw)
    col_map     = {name: f"col{i}" for i, name in enumerate(col_names)}

    grp_len = len(groups_raw) if hasattr(groups_raw, '__len__') else '?'
    print(f"  [{label}] DIAG keys={list(data.keys())} groupDefs={group_names} colNames={col_names} groups={type(groups_raw).__name__}[{grp_len}]")
    if isinstance(groups_raw, dict) and groups_raw:
        sample_k = list(groups_raw.keys())[:3]
        for k in sample_k:
            print(f"  [{label}] DIAG groups[{repr(k)}]={json.dumps(groups_raw[k])[:300]}")
    elif isinstance(groups_raw, list) and groups_raw:
        print(f"  [{label}] DIAG groups[0]={json.dumps(groups_raw[0])[:300]}")

    # Normalise groups to a dict
    if isinstance(groups_raw, list):
        if not groups_raw:
            return rows
        groups = {}
        for item in groups_raw:
            if isinstance(item, dict):
                user_key = (item.get("user") or item.get("name") or
                            item.get(group_names[0] if group_names else "user", ""))
                if user_key and str(user_key) not in ("total", "--", ""):
                    groups[str(user_key)] = item
    elif isinstance(groups_raw, dict):
        groups = groups_raw
    else:
        return rows

    if not groups:
        print(f"  [{label}] DIAG: groups empty after normalise")
        return rows

    # Detect nested structure (group0=date, group1=user)
    sample_keys      = [k for k in list(groups.keys())[:3] if k not in ("total", "--", "")]
    date_pattern     = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    looks_like_dates = bool(sample_keys) and all(date_pattern.match(str(k)) for k in sample_keys)

    if looks_like_dates and len(group_names) > 1:
        user_agg = {}
        for date_key, date_node in groups.items():
            if date_key in ("total", "--", "") or not isinstance(date_node, dict):
                continue
            inner_groups_raw = date_node.get("groups", {})
            if isinstance(inner_groups_raw, list):
                inner_groups = {}
                for item in inner_groups_raw:
                    if isinstance(item, dict):
                        uk = (item.get("user") or item.get("name") or
                              item.get(group_names[-1] if group_names else "user", ""))
                        if uk:
                            inner_groups[str(uk)] = item
            else:
                inner_groups = inner_groups_raw if isinstance(inner_groups_raw, dict) else {}

            inner_col_raw = date_node.get("columnDefs", col_defs_raw)
            inner_names   = _build_col_names(inner_col_raw)
            inner_map     = {name: f"col{i}" for i, name in enumerate(inner_names)}

            for user_key, stats in inner_groups.items():
                if user_key in ("total", "--", "") or not isinstance(stats, dict):
                    continue
                if user_key not in user_agg:
                    user_agg[user_key] = {"name": user_key, "completed": 0, "success": 0, "workTime": 0, "declines": 0}
                agg = user_agg[user_key]
                agg["completed"] += _safe_int(_get_col(stats, inner_map, "completed", "count"))
                agg["success"]   += _safe_int(_get_col(stats, inner_map, "success", "connects"))
                agg["workTime"]  += _safe_int(_get_col(stats, inner_map, "workTime"))
                agg["declines"]  += (
                    _safe_int(_get_col(stats, inner_map, "norespons", "noResponse")) +
                    _safe_int(_get_col(stats, inner_map, "answeringmachines", "answeringMachines"))
                )

        for agg in user_agg.values():
            agg["successRate"] = round(agg["success"] / agg["completed"] * 100, 1) if agg["completed"] > 0 else 0.0
        rows = list(user_agg.values())

    else:
        for user_key, stats in groups.items():
            if user_key in ("total", "--", "") or not isinstance(stats, dict):
                continue
            completed = _safe_int(_get_col(stats, col_map, "completed", "count"))
            success   = _safe_int(_get_col(stats, col_map, "success", "connects"))
            work_time = _safe_int(_get_col(stats, col_map, "workTime"))
            norespons = _safe_int(_get_col(stats, col_map, "norespons", "noResponse"))
            answering = _safe_int(_get_col(stats, col_map, "answeringmachines", "answeringMachines"))
            sr        = _safe_float(_get_col(stats, col_map, "successRate", "connectRate", "success_rate"))
            rows.append({
                "name":        user_key,
                "completed":   completed,
                "success":     success,
                "workTime":    work_time,
                "declines":    norespons + answering,
                "successRate": sr,
            })

    if rows:
        print(f"  [{label}] Extracted {len(rows)} user rows")
    else:
        print(f"  [{label}] DIAG: 0 rows from {len(groups)} groups")
    return rows


# -- Parse a raw row into a dashboard agent dict ------------------------------
def parse_row(row):
    if not isinstance(row, dict):
        return None

    name = str(row.get("name", row.get("user", ""))).strip()
    if not name or name.lower() in ("total", "--", "grand total"):
        return None

    calls     = _safe_int(row.get("completed", row.get("count", row.get("calls", 0))))
    success   = _safe_int(row.get("success", row.get("connects", 0)))
    declines  = _safe_int(row.get("declines", 0))
    work_secs = _safe_int(row.get("workTime", 0))

    if calls == 0:
        return None

    work_hrs = work_secs / 3600.0
    cph      = round(calls / work_hrs, 1) if work_hrs > 0 else 0.0
    sr       = row.get("successRate")
    if sr is None or sr == "":
        sr = round(success / calls * 100, 1) if calls > 0 else 0.0
    else:
        sr = _safe_float(sr)

    return {
        "name":        name,
        "calls":       calls,
        "success":     success,
        "declines":    declines,
        "cph":         cph,
        "successRate": sr,
        "workHours":   round(work_hrs, 2),
        "meetsTarget": cph >= BENCHMARKS["cph"],
    }


# -- Try timespans to get non-empty data --------------------------------------
def try_fetch_nonempty(base, token, label, group_by, columns, template="dialerStat"):
    """Try multiple timespan formats until we get non-empty groups."""
    timespans = [
        "0-0day",           # today
        "-1-0day",          # yesterday to today
        "-7-0day",          # last 7 days
        "-14-0day",         # last 14 days
        "-30-0day",         # last 30 days
        "0-7day",           # 0 to +7 (wrong direction but test)
        "0-14day",          # original attempt
    ]
    params_base = {"access_token": token, "asTree": "true"}
    for col_i, col_name in enumerate(columns):
        params_base[f"column{col_i}"] = col_name
    for grp_i, grp_name in enumerate(group_by):
        params_base[f"group{grp_i}"] = grp_name

    url = f"{base}/reports/{template}/report/{LOCALE}"

    for ts in timespans:
        params = {**params_base, "timespan": ts}
        data = fetch_json_report(url, params, label, f"TRY {template} group={group_by} ts={ts}")
        if data is None:
            return None  # 403
        if isinstance(data, dict):
            grp = data.get("groups", [])
            grp_len = len(grp) if hasattr(grp, '__len__') else 0
            is_empty = grp_len == 0
            print(f"  [{label}] ts={ts} groups={type(grp).__name__}[{grp_len}]{'  <-- empty' if is_empty else '  <-- HAS DATA!'}")
            if not is_empty:
                return data
    return []  # all empty


# -- Fetch stats for one campaign ---------------------------------------------
def fetch_report(campaign, index, total):
    cid   = campaign.get("id", "")
    token = campaign.get("token", "")
    label = f"{index + 1}/{total} {cid}"

    if not cid or not token:
        return []

    base = f"{API_BASE}/api/campaigns/{cid}"

    # Find the timespan that returns data: try editsDef_v2 by user
    print(f"  [{label}] Searching for timespan with data...")
    data1 = try_fetch_nonempty(base, token, label,
        group_by=["user"],
        columns=["completed", "success", "successRate", "workTime"],
        template="editsDef_v2")
    if data1 is None:
        print(f"  [{label}] 403 - token invalid for this campaign")
        return []
    if isinstance(data1, dict) and data1:
        rows = extract_rows_from_tree(data1, label)
        if rows:
            return rows

    # Try dialerStat
    data2 = try_fetch_nonempty(base, token, label,
        group_by=["user"],
        columns=["count", "connects", "answeringmachines", "norespons", "connectRate"],
        template="dialerStat")
    if data2 is None:
        return []
    if isinstance(data2, dict) and data2:
        rows = extract_rows_from_tree(data2, label)
        if rows:
            return rows

    print(f"  [{label}] All timespans returned empty - no calls in any period tested")
    return []


# -- Main ---------------------------------------------------------------------
def main():
    now_utc  = datetime.datetime.now(datetime.timezone.utc)
    now_sast = now_utc.astimezone(TIMEZONE)

    period_end   = now_sast.date()
    period_start = period_end - datetime.timedelta(days=DAYS_BACK)

    print("=== DialFire Weekly Fetch ===")
    print(f"Period : {period_start} to {period_end}  ({DAYS_BACK} days, asTree=true JSON)")

    tenant_id = os.environ.get("DIALFIRE_TENANT_ID", "")
    print(f"Tenant : {'***' if tenant_id else '(not set)'}")

    # Collect campaigns from CAMPAIGN_n_ID/TOKEN secrets
    campaigns = []
    i = 1
    while True:
        cid  = os.environ.get(f"CAMPAIGN_{i}_ID", "").strip()
        ctok = os.environ.get(f"CAMPAIGN_{i}_TOKEN", "").strip()
        if not cid:
            break
        if ctok:
            campaigns.append({"id": cid, "token": ctok})
            print(f"  Campaign {i}: {cid} (token=***)")
        else:
            print(f"  Campaign {i}: {cid} (NO TOKEN - skipping)")
        i += 1

    if not campaigns:
        print("No campaigns configured. Set CAMPAIGN_1_ID + CAMPAIGN_1_TOKEN secrets.")
        return

    print(f"Using {len(campaigns)} campaign(s)")
    print()

    # Fetch per-campaign rows
    all_rows = []
    for idx, campaign in enumerate(campaigns):
        rows = fetch_report(campaign, idx, len(campaigns))
        all_rows.extend(rows)

    print()
    print(f"Raw agent rows collected: {len(all_rows)}")

    # Merge rows by agent name
    merged = {}
    for row in all_rows:
        agent = parse_row(row)
        if agent is None:
            continue
        name = agent["name"]
        if name in merged:
            ex = merged[name]
            ex["calls"]     += agent["calls"]
            ex["success"]   += agent["success"]
            ex["declines"]  += agent["declines"]
            ex["workHours"]  = round(ex["workHours"] + agent["workHours"], 2)
            ex["cph"]        = round(ex["calls"] / ex["workHours"], 1) if ex["workHours"] > 0 else 0.0
            ex["successRate"]= round(ex["success"] / ex["calls"] * 100, 1) if ex["calls"] > 0 else 0.0
            ex["meetsTarget"]= ex["cph"] >= BENCHMARKS["cph"]
        else:
            merged[name] = agent

    print(f"Unique agents after merge: {len(merged)}")

    rm_agents    = []
    fancy_agents = []
    for name, agent in sorted(merged.items()):
        if name in RM_NAMES:
            rm_agents.append(agent)
        else:
            fancy_agents.append(agent)

    rm_agents.sort(key=lambda x: x["calls"], reverse=True)
    fancy_agents.sort(key=lambda x: x["calls"], reverse=True)

    print(f"RM: {len(rm_agents)} | Fancy Callers: {len(fancy_agents)}")
    if rm_agents:
        print("RM agents:")
        for a in rm_agents:
            print(f"  {a['name']:25s} calls={a['calls']:4d}  success={a['success']:4d}  declines={a['declines']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")
    if fancy_agents:
        print("Fancy Callers (top 15):")
        for a in fancy_agents[:15]:
            print(f"  {a['name']:25s} calls={a['calls']:4d}  success={a['success']:4d}  declines={a['declines']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")

    # Build output
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    weekly_path = os.path.join(data_dir, "weekly_data.json")
    weekly = {
        "generated":   now_utc.isoformat(),
        "periodStart": str(period_start),
        "periodEnd":   str(period_end),
        "rm":          rm_agents,
        "fancy":       fancy_agents,
    }
    with open(weekly_path, "w") as f:
        json.dump(weekly, f, indent=2)
    print(f"Saved weekly_data.json  (rm={len(rm_agents)}, fancy={len(fancy_agents)})")

    # Update history
    history_path = os.path.join(data_dir, "history.json")
    try:
        with open(history_path) as f:
            raw_hist = json.load(f)
    except Exception:
        raw_hist = []
    history = [h for h in raw_hist if isinstance(h, dict) and "weekStart" in h]

    week_key = str(period_start)
    history  = [h for h in history if h.get("weekStart") != week_key]
    history.append({
        "weekStart": week_key,
        "weekEnd":   str(period_end),
        "generated": now_utc.isoformat(),
        "rm":        rm_agents,
        "fancy":     fancy_agents,
    })
    history.sort(key=lambda x: x["weekStart"])
    history = history[-12:]

    with open(history_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Updated history.json  ({len(history)} weeks)")


if __name__ == "__main__":
    main()
