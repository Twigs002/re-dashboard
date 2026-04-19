"""
DialFire Multi-Campaign -> weekly_data.json fetcher
=====================================================
Uses tenant API to discover all campaigns, then fetches
per-campaign agent statistics via the editsDef_v2 report.

KEY FIX (DialFire support confirmed):
  - Use asTree=true  ->  API returns JSON  (NOT CSV)
  - Use asTree=false ->  API returns CSV
  - Correct endpoint: /reports/{template}/report/{locale}
  - Auth: access_token={campaign_token}  (query param)
  - Example URL from DialFire support:
    /reports/editsDef_v2/metadata/de_DE?asTree=true&group0=date&group1=user
      &column0=completed&column1=success&column2=successRate
      &column3=success_p_h&column4=completed_p_h&column5=workTime
      &column6=talkTimeDialerShare&days=30&_token_={Campaign_Token}

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
        "DIALFIRE_TENANT_ID and DIALFIRE_TENANT_TOKEN secrets must be set."
    )


# ── Date range: last 7 days ───────────────────────────────────────────────────
DAYS_BACK = 7
now_utc   = datetime.now(timezone.utc)
DATE_FROM = (now_utc - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
DATE_TO   = now_utc.strftime("%Y-%m-%d")

# Locale with SAST timezone
LOCALE = "de_DE/Africa/Johannesburg"

# ── Agent classification ──────────────────────────────────────────────────────
RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

BENCHMARKS = {
    "cph":            45,
    "daily_calls":    315,
    "rm_success_rate": 17.0,
    "fc_success_rate": 20.0,
}


# ── Tenant API: list all campaigns ───────────────────────────────────────────
def get_all_campaigns():
    url     = f"https://api.dialfire.com/api/tenants/{TENANT_ID}/campaigns"
    headers = {"Authorization": f"Bearer {TENANT_TOKEN}"}
    r = requests.get(url, headers=headers, timeout=30)
    print(f"Tenant campaigns -> HTTP {r.status_code}")
    if r.status_code != 200:
        print(f"  Body: {r.text[:300]}")
        return []
    data = r.json()
    # API may return a list or a dict with a list
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("campaigns", "data", "items", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return list(data.values()) if data else []
    return []


# ── Fetch JSON report using asTree=true ──────────────────────────────────────
def fetch_json_report(url, params, label, tag):
    """
    Fetch a report with asTree=true to get JSON back.
    Handles HTTP 202 (async, poll up to 5x with 5s delay).
    Returns parsed JSON data or None on failure.
    """
    # Always request JSON
    params = dict(params)
    params["asTree"] = "true"

    try:
        r = requests.get(url, params=params, timeout=30)
        status_line = f"  [{label}] {tag} -> HTTP {r.status_code}"

        # ── 202: async report, poll ───────────────────────────────────────────
        if r.status_code == 202:
            print(f"{status_line}  (async, polling...)")
            for attempt in range(6):
                time.sleep(5)
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    break
                if r.status_code == 202:
                    print(f"    [{label}] still 202, attempt {attempt+1}/6...")
                    continue
                break
            status_line = f"  [{label}] {tag} -> HTTP {r.status_code} (after poll)"

        if r.status_code == 401:
            print(f"{status_line}  (bad token - skipping campaign)")
            return None  # Signal: bad token, skip this campaign

        if r.status_code == 500:
            print(f"{status_line}  (server error - template not available)")
            return []

        if r.status_code != 200:
            print(f"{status_line}  (unexpected status)")
            return []

        ct = r.headers.get("Content-Type", "")
        print(f"{status_line}  content-type={ct}")

        try:
            data = r.json()
            return data
        except Exception as e:
            print(f"    [{label}] JSON parse failed: {e}")
            print(f"    First 200 chars: {r.text[:200]}")
            return []

    except Exception as e:
        print(f"  [{label}] {tag} -> Exception: {e}")
        return []


# ── Extract agent rows from JSON tree response ────────────────────────────────
def extract_rows_from_tree(data, label):
    """
    DialFire asTree=true JSON structure:
    When group0=user: top-level keys are user names, values are dicts of stats.
    When group0=date&group1=user: top-level keys are dates, values are dicts
      with user names as keys, each having stats.

    We want per-user aggregated stats.
    Returns list of dicts: [{"name": user, "completed": N, "success": N,
                              "workTime": seconds, ...}, ...]
    """
    rows = []

    if not isinstance(data, dict) or not data:
        print(f"  [{label}] Empty or non-dict response: {type(data)}")
        return rows

    # Detect structure: does it have a "children" key (tree format)?
    if "children" in data:
        data = data["children"]

    # Check if top-level keys look like dates (YYYY-MM-DD) or user names
    sample_keys = list(data.keys())[:3]
    print(f"  [{label}] Top-level keys sample: {sample_keys}")

    # Try to figure out structure: group0=user (keys=users) or group0=date (keys=dates)
    import re
    date_pattern = re.compile(r'^d{4}-d{2}-d{2}$')
    looks_like_dates = all(date_pattern.match(str(k)) for k in sample_keys if k not in ('total', '--', ''))

    if looks_like_dates:
        # Structure: {date: {user: {stats}}}  (group0=date, group1=user)
        user_agg = {}
        for date_key, user_dict in data.items():
            if date_key in ('total', '--', ''):
                continue
            if not isinstance(user_dict, dict):
                continue
            for user_key, stats in user_dict.items():
                if user_key in ('total', '--', ''):
                    continue
                if not isinstance(stats, dict):
                    continue
                if user_key not in user_agg:
                    user_agg[user_key] = {"name": user_key, "completed": 0, "success": 0,
                                          "workTime": 0, "successRate": 0, "_count": 0}
                agg = user_agg[user_key]
                agg["completed"] += _safe_int(stats.get("completed", stats.get("count", 0)))
                agg["success"]   += _safe_int(stats.get("success", 0))
                agg["workTime"]  += _safe_int(stats.get("workTime", 0))
                agg["_count"]    += 1
        # Calculate average success rate
        for agg in user_agg.values():
            if agg["completed"] > 0:
                agg["successRate"] = round(agg["success"] / agg["completed"] * 100, 1)
        rows = list(user_agg.values())
    else:
        # Structure: {user: {stats}}  (group0=user)
        for user_key, stats in data.items():
            if user_key in ('total', '--', ''):
                continue
            if not isinstance(stats, dict):
                continue
            rows.append({
                "name":        user_key,
                "completed":   _safe_int(stats.get("completed", stats.get("count", 0))),
                "success":     _safe_int(stats.get("success", 0)),
                "workTime":    _safe_int(stats.get("workTime", 0)),
                "successRate": _safe_float(stats.get("successRate", stats.get("success_rate", 0))),
            })

    print(f"  [{label}] Extracted {len(rows)} user rows from tree")
    return rows


def _safe_int(v):
    try:
        return int(float(str(v).replace('%', '').strip()))
    except Exception:
        return 0

def _safe_float(v):
    try:
        return float(str(v).replace('%', '').strip())
    except Exception:
        return 0.0


# ── Parse a single agent row into dashboard format ───────────────────────────
def parse_row(row, label):
    """
    Convert a row dict into dashboard agent format.
    Returns None if the agent has 0 calls (skip).
    """
    name = str(row.get("name", row.get("user", row.get("key", "")))).strip()

    # Skip totals / blanks
    if not name or name.lower() in ("total", "--", "grand total"):
        return None

    calls      = _safe_int(row.get("completed", row.get("count", 0)))
    success    = _safe_int(row.get("success", 0))
    work_secs  = _safe_int(row.get("workTime", 0))

    if calls == 0:
        return None

    work_hrs   = work_secs / 3600.0 if work_secs > 3600 else work_secs / 60.0
    # workTime field: some templates return seconds, some return minutes
    # DialFire docs show workTime as HH:MM:SS string sometimes, or numeric seconds
    # We detect: if numeric and > 10000 it's likely seconds
    if work_secs > 86400:  # more than 24 hours in seconds = definitely seconds
        work_hrs = work_secs / 3600.0
    elif work_secs > 1440:  # more than 24 hours in minutes = likely seconds
        work_hrs = work_secs / 3600.0
    elif work_secs > 0:
        work_hrs = work_secs / 60.0  # assume minutes

    cph        = round(calls / work_hrs, 1) if work_hrs > 0 else 0
    sr         = _safe_float(row.get("successRate", row.get("success_rate", 0)))
    if sr == 0 and calls > 0:
        sr = round(success / calls * 100, 1)

    return {
        "name":         name,
        "calls":        calls,
        "success":      success,
        "cph":          cph,
        "successRate":  sr,
        "workHours":    round(work_hrs, 2),
        "meetsTarget":  cph >= BENCHMARKS["cph"],
    }


# ── Fetch stats for one campaign ─────────────────────────────────────────────
def fetch_report(campaign, index, total):
    cid   = campaign.get("id", campaign.get("_id", ""))
    cname = campaign.get("name", cid)
    token = campaign.get("permissions", {}).get("token", "")

    label = f"{index+1}/{total} {cname[:30]}"

    if not token:
        print(f"  [{label}] No campaign token - skipping")
        return []

    base = f"https://api.dialfire.com/api/campaigns/{cid}"

    # Common params for all attempts
    base_params = {
        "access_token": token,
        "timespan":     f"0-{DAYS_BACK}day",
        "asTree":       "true",
    }

    # ── Strategy 1: editsDef_v2/report with group0=user ──────────────────────
    # This matches the DialFire example URL exactly, using report (not metadata)
    url1    = f"{base}/reports/editsDef_v2/report/{LOCALE}"
    params1 = {
        **base_params,
        "group0":   "user",
        "column0":  "completed",
        "column1":  "success",
        "column2":  "successRate",
        "column3":  "workTime",
        "column4":  "success_p_h",
        "column5":  "completed_p_h",
    }
    data1 = fetch_json_report(url1, params1, label, "editsDef_v2/report[group=user]")
    if data1 is None:
        return []  # 401 - bad token, skip
    if data1:
        rows = extract_rows_from_tree(data1, label)
        if rows:
            print(f"  [{label}] editsDef_v2/report -> {len(rows)} rows ✓")
            return rows

    # ── Strategy 2: dialerStat/report with group0=user ───────────────────────
    url2    = f"{base}/reports/dialerStat/report/{LOCALE}"
    params2 = {
        **base_params,
        "group0":  "user",
        "column0": "count",
        "column1": "connects",
        "column2": "answeringmachines",
        "column3": "norespons",
        "column4": "invalid",
        "column5": "connectRate",
    }
    data2 = fetch_json_report(url2, params2, label, "dialerStat/report[group=user]")
    if data2 is None:
        return []  # 401
    if data2:
        rows = extract_rows_from_tree(data2, label)
        if rows:
            print(f"  [{label}] dialerStat/report -> {len(rows)} rows ✓")
            return rows

    # ── Strategy 3: editsDef_v2/metadata with group0=date&group1=user ────────
    # (metadata endpoint - DialFire example showed this works)
    url3    = f"{base}/reports/editsDef_v2/metadata/{LOCALE}"
    params3 = {
        **base_params,
        "group0":  "date",
        "group1":  "user",
        "column0": "completed",
        "column1": "success",
        "column2": "successRate",
        "column3": "workTime",
    }
    data3 = fetch_json_report(url3, params3, label, "editsDef_v2/metadata[group=date,user]")
    if data3 is None:
        return []
    if data3:
        rows = extract_rows_from_tree(data3, label)
        if rows:
            print(f"  [{label}] editsDef_v2/metadata -> {len(rows)} rows ✓")
            return rows

    # ── Strategy 4: dialerStat/report with group0=date&group1=user ───────────
    url4    = f"{base}/reports/dialerStat/report/{LOCALE}"
    params4 = {
        **base_params,
        "group0":  "date",
        "group1":  "user",
        "column0": "count",
        "column1": "connects",
        "column2": "connectRate",
    }
    data4 = fetch_json_report(url4, params4, label, "dialerStat/report[group=date,user]")
    if data4 is None:
        return []
    if data4:
        rows = extract_rows_from_tree(data4, label)
        if rows:
            print(f"  [{label}] dialerStat/report[date,user] -> {len(rows)} rows ✓")
            return rows

    print(f"  [{label}] All strategies returned 0 rows")
    return []


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"=== DialFire Weekly Fetch ===")
    print(f"Period : {DATE_FROM} to {DATE_TO}  ({DAYS_BACK} days, asTree=true JSON)")
    print(f"Tenant : {TENANT_ID}")
    print()

    # 1. Get all campaigns
    campaigns = get_all_campaigns()
    if not campaigns:
        print("ERROR: No campaigns returned from tenant API")
        raise SystemExit(1)

    print(f"Found {len(campaigns)} campaigns total")

    # Filter to active only
    active = [c for c in campaigns if c.get("status", "active") not in ("deleted", "archived")]
    print(f"Active campaigns: {len(active)}")
    print()

    # 2. Fetch per-campaign stats
    all_rows = []
    for i, campaign in enumerate(active):
        rows = fetch_report(campaign, i, len(active))
        all_rows.extend(rows)

    print()
    print(f"Raw agent rows collected: {len(all_rows)}")
    agent_rows_with_calls = [r for r in all_rows if r.get("calls", 0) > 0]
    print(f"Raw agent rows with calls > 0: {len(agent_rows_with_calls)}")

    # 3. Merge duplicate agent names (same agent in multiple campaigns)
    merged = {}
    for row in all_rows:
        parsed = parse_row(row, "merge")
        if not parsed:
            continue
        name = parsed["name"]
        if name not in merged:
            merged[name] = parsed
        else:
            # Aggregate across campaigns
            existing = merged[name]
            existing["calls"]    += parsed["calls"]
            existing["success"]  += parsed["success"]
            existing["workHours"] = round(existing["workHours"] + parsed["workHours"], 2)
            # Recalculate CPH and success rate
            if existing["workHours"] > 0:
                existing["cph"] = round(existing["calls"] / existing["workHours"], 1)
            if existing["calls"] > 0:
                existing["successRate"] = round(existing["success"] / existing["calls"] * 100, 1)
            existing["meetsTarget"] = existing["cph"] >= BENCHMARKS["cph"]

    print(f"Unique agents after merge: {len(merged)}")

    # 4. Split into RM and Fancy Callers
    rm_agents     = []
    fancy_agents  = []

    for name, agent in sorted(merged.items()):
        if name in RM_NAMES:
            rm_agents.append(agent)
        else:
            fancy_agents.append(agent)

    rm_agents.sort(key=lambda x: x["calls"], reverse=True)
    fancy_agents.sort(key=lambda x: x["calls"], reverse=True)

    print(f"RM: {len(rm_agents)} | Fancy Callers: {len(fancy_agents)}")
    print()
    if rm_agents:
        print("RM agents:")
        for a in rm_agents:
            print(f"  {a['name']:25s} calls={a['calls']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")
    if fancy_agents:
        print("Fancy Caller agents (top 10):")
        for a in fancy_agents[:10]:
            print(f"  {a['name']:25s} calls={a['calls']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")

    # 5. Build weekly_data.json
    weekly_data = {
        "weekStart":    DATE_FROM,
        "weekEnd":      DATE_TO,
        "lastUpdated":  datetime.now(timezone.utc).isoformat(),
        "benchmarks":   BENCHMARKS,
        "rm":           rm_agents,
        "fancy":        fancy_agents,
    }

    out_path = os.path.join(os.path.dirname(__file__), "..", "data", "weekly_data.json")
    out_path = os.path.normpath(out_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(weekly_data, f, indent=2)
    print(f"Saved weekly_data.json  (rm={len(rm_agents)}, fancy={len(fancy_agents)})")

    # 6. Update history.json
    hist_path = os.path.join(os.path.dirname(__file__), "..", "data", "history.json")
    hist_path = os.path.normpath(hist_path)
    if os.path.exists(hist_path):
        with open(hist_path) as f:
            history = json.load(f)
    else:
        history = []

    week_entry = {
        "weekStart": DATE_FROM,
        "weekEnd":   DATE_TO,
        "rm":        len(rm_agents),
        "fancy":     len(fancy_agents),
    }
    # Replace existing entry for this week if present
    history = [h for h in history if h.get("weekStart") != DATE_FROM]
    history.append(week_entry)
    history.sort(key=lambda h: h.get("weekStart", ""), reverse=True)
    history = history[:52]  # keep last 52 weeks

    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Updated history.json  ({len(history)} weeks)")


if __name__ == "__main__":
    main()
