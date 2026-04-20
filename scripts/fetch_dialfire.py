"""
DialFire Multi-Campaign -> weekly_data.json fetcher
=====================================================
Uses tenant API to discover all campaigns, then fetches
per-campaign agent statistics via the editsDef_v2 report.

KEY FIX (DialFire support confirmed):
  - Use asTree=true  ->  API returns JSON  (NOT CSV)
  - JSON structure: {
      "groups": {user: {"col0": val, "col1": val, ...}},
      "groupDefs": [{"name": "user", "title": "..."}, ...],
      "columnDefs": [{"name": "completed", "title": "...", "conf": {}}, ...]
    }
  - groups key may be absent if campaign had 0 calls in this period

Secrets required:
  DIALFIRE_TENANT_ID    - e.g. 3f88c548
  DIALFIRE_TENANT_TOKEN - tenant-level Bearer token
"""

import os, json, time, requests, re
from datetime import datetime, timedelta, timezone


TENANT_ID    = os.environ.get("DIALFIRE_TENANT_ID", "").strip()
TENANT_TOKEN = os.environ.get("DIALFIRE_TENANT_TOKEN", "").strip()


if not TENANT_ID or not TENANT_TOKEN:
    raise ValueError(
        "DIALFIRE_TENANT_ID and DIALFIRE_TENANT_TOKEN secrets must be set."
    )


# ── Date range: last 14 days (wider net to catch active campaigns) ────────────
DAYS_BACK = 14
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
    url     = f"https://api.dialfire.com/api/tenants/{TENANT_ID}/campaigns/"
    headers = {"Authorization": f"Bearer {TENANT_TOKEN}"}

    print(f"Fetching campaign list from tenant API...")
    print(f"  URL: {url.replace(TENANT_ID, '***')}")

    try:
        r = requests.get(url, headers=headers, timeout=30)
        print(f"  Tenant API -> HTTP {r.status_code}")

        if r.status_code == 403:
            print(f"  Body: {r.text[:500]}")
            print("  *** TENANT TOKEN IS INVALID OR EXPIRED ***")
            r2 = requests.get(url, params={"access_token": TENANT_TOKEN}, timeout=30)
            print(f"  Tenant API (access_token param) -> HTTP {r2.status_code}")
            if r2.status_code == 200:
                return _parse_campaigns(r2.json())
            return []

        if r.status_code != 200:
            print(f"  ERROR: {r.text[:300]}")
            return []

        return _parse_campaigns(r.json())

    except Exception as e:
        print(f"  FAIL tenant API: {e}")
        return []


def _parse_campaigns(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("campaigns", "data", "items", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return list(data.values()) if data else []
    return []


# ── Helper: extract column names from DialFire columnDefs ────────────────────
def _extract_col_names(col_defs_raw):
    """columnDefs can be list of strings or list of {name, title, conf} objects."""
    if not col_defs_raw:
        return []
    result = []
    for item in col_defs_raw:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            result.append(item.get("name", ""))
        else:
            result.append(str(item))
    return result


# ── Fetch JSON report using asTree=true ──────────────────────────────────────
def fetch_json_report(url, params, label, tag):
    """Fetch a report with asTree=true. Returns JSON data or None on 401."""
    params = dict(params)
    params["asTree"] = "true"

    try:
        r = requests.get(url, params=params, timeout=30)
        status_line = f"  [{label}] {tag} -> HTTP {r.status_code}"

        if r.status_code == 202:
            print(f"{status_line}  (async, polling...)")
            for attempt in range(3):  # Max 3 attempts x 3s = 9s
                time.sleep(3)
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    break
                if r.status_code == 202:
                    continue
                break
            status_line = f"  [{label}] {tag} -> HTTP {r.status_code} (after poll)"

        if r.status_code == 401:
            print(f"{status_line}  (bad token)")
            return None

        if r.status_code in (404, 500):
            print(f"{status_line}  (skip)")
            return []

        if r.status_code != 200:
            print(f"{status_line}  (status {r.status_code})")
            return []

        ct = r.headers.get("Content-Type", "")
        print(f"{status_line}  ct={ct[:30]}")

        try:
            data = r.json()
            return data
        except Exception as e:
            print(f"    [{label}] JSON parse error: {e} / {r.text[:80]}")
            return []

    except Exception as e:
        print(f"  [{label}] {tag} -> Exception: {e}")
        return []


# ── Extract agent rows from DialFire asTree=true JSON ────────────────────────
def extract_rows_from_tree(data, label, first_campaign=False):
    """
    Parse DialFire asTree=true JSON into user rows.
    Structure: {"groups": {user: {col0:val,...}}, "groupDefs": [...], "columnDefs": [...]}
    """
    rows = []

    if not isinstance(data, dict):
        return rows

    col_defs_raw   = data.get("columnDefs", [])
    group_defs_raw = data.get("groupDefs", [])
    groups         = data.get("groups", data.get("children", {}))

    col_names   = _extract_col_names(col_defs_raw)
    group_names = _extract_col_names(group_defs_raw)

    # Print diagnostic for first campaign or when we find data
    if first_campaign:
        print(f"  [{label}] DIAG groupDefs={group_names} colNames={col_names[:6]}")
        print(f"  [{label}] DIAG top-level keys: {list(data.keys())}")
        if groups:
            first_key = list(groups.keys())[0] if groups else None
            first_val = groups.get(first_key, {}) if first_key else {}
            print(f"  [{label}] DIAG sample group key='{first_key}' val_keys={list(first_val.keys())[:6] if isinstance(first_val, dict) else type(first_val)}")
        else:
            # Print raw data excerpt when no groups
            raw = json.dumps(data)[:300]
            print(f"  [{label}] DIAG no groups, raw excerpt: {raw}")

    if not groups:
        return rows

    col_map = {name: f"col{i}" for i, name in enumerate(col_names)}

    # Check nesting depth
    sample_keys = [k for k in list(groups.keys())[:3] if k not in ('total', '--', '')]
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    looks_like_dates = bool(sample_keys) and all(date_pattern.match(str(k)) for k in sample_keys)

    if looks_like_dates and len(group_names) > 1:
        # Nested: {date: {"groups": {user: stats}}}
        user_agg = {}
        for date_key, date_node in groups.items():
            if date_key in ('total', '--', '') or not isinstance(date_node, dict):
                continue
            inner_groups  = date_node.get("groups", {})
            inner_col_raw = date_node.get("columnDefs", col_defs_raw)
            inner_names   = _extract_col_names(inner_col_raw)
            inner_map     = {name: f"col{i}" for i, name in enumerate(inner_names)}

            for user_key, stats in inner_groups.items():
                if user_key in ('total', '--', '') or not isinstance(stats, dict):
                    continue
                if user_key not in user_agg:
                    user_agg[user_key] = {"name": user_key, "completed": 0, "success": 0, "workTime": 0}
                agg = user_agg[user_key]
                agg["completed"] += _safe_int(_get_col(stats, inner_map, "completed", "count"))
                agg["success"]   += _safe_int(_get_col(stats, inner_map, "success"))
                agg["workTime"]  += _safe_int(_get_col(stats, inner_map, "workTime"))

        for agg in user_agg.values():
            agg["successRate"] = round(agg["success"] / agg["completed"] * 100, 1) if agg["completed"] > 0 else 0.0
        rows = list(user_agg.values())

    else:
        # Flat: {user: {col0: val, col1: val, ...}}
        for user_key, stats in groups.items():
            if user_key in ('total', '--', '') or not isinstance(stats, dict):
                continue
            completed = _safe_int(_get_col(stats, col_map, "completed", "count"))
            success   = _safe_int(_get_col(stats, col_map, "success"))
            work_time = _safe_int(_get_col(stats, col_map, "workTime"))
            sr        = _safe_float(_get_col(stats, col_map, "successRate", "success_rate", default=0))
            rows.append({
                "name":        user_key,
                "completed":   completed,
                "success":     success,
                "workTime":    work_time,
                "successRate": sr,
            })

    if rows:
        print(f"  [{label}] Extracted {len(rows)} user rows")
    return rows


def _get_col(stats, col_map, *names, default=0):
    for name in names:
        mapped = col_map.get(name)
        if mapped and mapped in stats:
            return stats[mapped]
        if name in stats:
            return stats[name]
    return default


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


# ── Parse an agent row into dashboard format ─────────────────────────────────
def parse_row(row):
    name = str(row.get("name", row.get("user", ""))).strip()
    if not name or name.lower() in ("total", "--", "grand total"):
        return None

    calls     = _safe_int(row.get("completed", row.get("count", row.get("calls", 0))))
    success   = _safe_int(row.get("success", 0))
    work_secs = _safe_int(row.get("workTime", 0))

    if calls == 0:
        return None

    work_hrs = work_secs / 3600.0 if work_secs > 0 else 0
    cph = round(calls / work_hrs, 1) if work_hrs > 0 else 0
    sr  = _safe_float(row.get("successRate", 0))
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
    label = f"{index+1}/{total} {cname[:20]}"
    is_first = (index == 0)

    if not token:
        return []

    base = f"https://api.dialfire.com/api/campaigns/{cid}"
    base_params = {"access_token": token, "timespan": f"0-{DAYS_BACK}day"}

    # Strategy 1: editsDef_v2/report group0=user
    data1 = fetch_json_report(
        f"{base}/reports/editsDef_v2/report/{LOCALE}",
        {**base_params, "group0": "user",
         "column0": "completed", "column1": "success", "column2": "successRate",
         "column3": "workTime", "column4": "success_p_h", "column5": "completed_p_h"},
        label, "editsDef_v2/report[user]"
    )
    if data1 is None: return []
    if isinstance(data1, dict) and data1:
        rows = extract_rows_from_tree(data1, label, is_first)
        if rows: return rows

    # Strategy 2: dialerStat/report group0=user
    data2 = fetch_json_report(
        f"{base}/reports/dialerStat/report/{LOCALE}",
        {**base_params, "group0": "user",
         "column0": "count", "column1": "connects", "column2": "norespons", "column3": "connectRate"},
        label, "dialerStat/report[user]"
    )
    if data2 is None: return []
    if isinstance(data2, dict) and data2:
        rows = extract_rows_from_tree(data2, label, False)
        if rows: return rows

    # Strategy 3: editsDef_v2/metadata group0=date group1=user (was working in run #20)
    data3 = fetch_json_report(
        f"{base}/reports/editsDef_v2/metadata/{LOCALE}",
        {**base_params, "group0": "date", "group1": "user",
         "column0": "completed", "column1": "success",
         "column2": "successRate", "column3": "workTime"},
        label, "editsDef_v2/metadata[date,user]"
    )
    if data3 is None: return []
    if isinstance(data3, dict) and data3:
        rows = extract_rows_from_tree(data3, label, is_first)
        if rows: return rows

    return []


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"=== DialFire Weekly Fetch ===")
    print(f"Period : {DATE_FROM} to {DATE_TO}  ({DAYS_BACK} days, asTree=true JSON)")
    print(f"Tenant : {TENANT_ID}")
    print()

    campaigns = get_all_campaigns()
    if not campaigns:
        print("FATAL: No campaigns returned from tenant API.")
        raise SystemExit(1)

    print(f"Found {len(campaigns)} campaigns total")
    active = [c for c in campaigns if c.get("status", "active") not in ("deleted", "archived")]
    print(f"Active campaigns: {len(active)}")
    print()

    all_rows = []
    for i, campaign in enumerate(active):
        rows = fetch_report(campaign, i, len(active))
        all_rows.extend(rows)

    print()
    print(f"Raw agent rows collected: {len(all_rows)}")
    rows_with_calls = [r for r in all_rows if _safe_int(r.get("completed", r.get("calls", 0))) > 0]
    print(f"Raw agent rows with calls > 0: {len(rows_with_calls)}")

    merged = {}
    for row in all_rows:
        parsed = parse_row(row)
        if not parsed:
            continue
        name = parsed["name"]
        if name not in merged:
            merged[name] = parsed
        else:
            ex = merged[name]
            ex["calls"]    += parsed["calls"]
            ex["success"]  += parsed["success"]
            ex["workHours"] = round(ex["workHours"] + parsed["workHours"], 2)
            if ex["workHours"] > 0:
                ex["cph"] = round(ex["calls"] / ex["workHours"], 1)
            if ex["calls"] > 0:
                ex["successRate"] = round(ex["success"] / ex["calls"] * 100, 1)
            ex["meetsTarget"] = ex["cph"] >= BENCHMARKS["cph"]

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
            print(f"  {a['name']:25s} calls={a['calls']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")
    if fancy_agents:
        print("Fancy Callers (top 10):")
        for a in fancy_agents[:10]:
            print(f"  {a['name']:25s} calls={a['calls']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")

    weekly_data = {
        "weekStart":   DATE_FROM,
        "weekEnd":     DATE_TO,
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "benchmarks":  BENCHMARKS,
        "rm":          rm_agents,
        "fancy":       fancy_agents,
    }

    out_path = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data", "weekly_data.json"))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(weekly_data, f, indent=2)
    print(f"Saved weekly_data.json  (rm={len(rm_agents)}, fancy={len(fancy_agents)})")

    hist_path = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data", "history.json"))
    if os.path.exists(hist_path):
        with open(hist_path) as f:
            history = json.load(f)
    else:
        history = []

    week_entry = {"weekStart": DATE_FROM, "weekEnd": DATE_TO, "rm": len(rm_agents), "fancy": len(fancy_agents)}
    history = [h for h in history if h.get("weekStart") != DATE_FROM]
    history.append(week_entry)
    history.sort(key=lambda h: h.get("weekStart", ""), reverse=True)
    history = history[:52]

    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Updated history.json  ({len(history)} weeks)")


if __name__ == "__main__":
    main()
