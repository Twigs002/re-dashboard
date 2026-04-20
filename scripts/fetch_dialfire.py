"""
DialFire Campaign Stats -> weekly_data.json fetcher
====================================================
Uses per-campaign tokens to fetch agent statistics.

API endpoint (confirmed by DialFire support):
  GET /api/campaigns/{campaign_id}/reports/{template}/report/{locale}
  ?asTree=true  -> returns JSON
  ?asTree=false -> returns CSV

JSON response structure:
  {
    "groups": {user_name: {"col0": val, "col1": val, ...}}
             OR [] if no data for this period
             OR {date: {"groups": {user: {...}}}}
    "groupDefs": [{"name": "user", ...}, ...]
    "columnDefs": [{"name": "completed", ...}, ...]
  }

GitHub Secrets required:
  DIALFIRE_TENANT_ID     - tenant id (e.g. 3f88c548)
  DIALFIRE_TENANT_TOKEN  - tenant-level Bearer token (for campaign discovery)

  Per-campaign (preferred - faster, avoids 246-campaign scan):
  CAMPAIGN_1_ID          - campaign id  (e.g. DXX5XQHGZ3R4W6R3)
  CAMPAIGN_1_TOKEN       - campaign-level access token
  CAMPAIGN_2_ID          - campaign id  (e.g. N9EA67VHYX6HZHFG)
  CAMPAIGN_2_TOKEN       - campaign-level access token
"""

import os, json, time, requests, re
from datetime import datetime, timedelta, timezone


# ── Config ────────────────────────────────────────────────────────────────────
TENANT_ID    = os.environ.get("DIALFIRE_TENANT_ID", "").strip()
TENANT_TOKEN = os.environ.get("DIALFIRE_TENANT_TOKEN", "").strip()

DAYS_BACK = 14
now_utc   = datetime.now(timezone.utc)
DATE_FROM = (now_utc - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
DATE_TO   = now_utc.strftime("%Y-%m-%d")

LOCALE = "de_DE/Africa/Johannesburg"

RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

BENCHMARKS = {
    "cph":             45,
    "daily_calls":     315,
    "rm_success_rate":  17.0,
    "fc_success_rate":  20.0,
}


# ── Build campaign list from env ──────────────────────────────────────────────
def get_campaigns_from_env():
    campaigns = []
    i = 1
    while True:
        cid   = os.environ.get(f"CAMPAIGN_{i}_ID", "").strip()
        token = os.environ.get(f"CAMPAIGN_{i}_TOKEN", "").strip()
        if not cid or not token:
            break
        campaigns.append({"id": cid, "token": token})
        i += 1
    return campaigns


def get_campaigns_from_tenant():
    if not TENANT_ID or not TENANT_TOKEN:
        return []
    url     = f"https://api.dialfire.com/api/tenants/{TENANT_ID}/campaigns/"
    headers = {"Authorization": f"Bearer {TENANT_TOKEN}"}
    print(f"  Discovering campaigns via tenant API: {url.replace(TENANT_ID, '***')}")
    try:
        r = requests.get(url, headers=headers, timeout=30)
        print(f"  Tenant API -> HTTP {r.status_code}")
        if r.status_code != 200:
            return []
        data = r.json()
        raw = _parse_campaigns(data)
        result = []
        for c in raw:
            if isinstance(c, dict):
                cid   = c.get("id", c.get("campaignId", ""))
                token = c.get("token", c.get("access_token", TENANT_TOKEN))
                if cid:
                    result.append({"id": cid, "token": token})
        return result
    except Exception as e:
        print(f"  Tenant API exception: {e}")
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


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def fetch_json_report(url, params, label, tag, max_polls=5, poll_wait=4):
    all_params = {**params, "asTree": "true"}
    try:
        r = requests.get(url, params=all_params, timeout=45)
        status_line = f"  [{label}] {tag} -> HTTP {r.status_code}"

        if r.status_code == 202:
            print(f"{status_line}  (async, polling...)")
            for _ in range(max_polls):
                time.sleep(poll_wait)
                r = requests.get(url, params=all_params, timeout=45)
                status_line = f"  [{label}] {tag} -> HTTP {r.status_code} (after poll)"
                if r.status_code != 202:
                    break
            if r.status_code == 202:
                print(f"{status_line}  (still 202 after polling, skip)")
                return []

        if r.status_code == 401:
            print(f"{status_line}  (bad token)")
            return None

        if r.status_code in (403, 404, 500):
            print(f"{status_line}  (skip)")
            return []

        if r.status_code != 200:
            print(f"{status_line}  (unexpected status {r.status_code})")
            return []

        ct = r.headers.get("Content-Type", "")
        print(f"{status_line}  ct={ct[:40]}")

        try:
            return r.json()
        except Exception as e:
            print(f"    [{label}] JSON parse error: {e} | body={r.text[:120]}")
            return []

    except Exception as e:
        print(f"  [{label}] {tag} -> Exception: {e}")
        return []


# ── Column name helpers ───────────────────────────────────────────────────────
def _extract_col_names(col_defs_raw):
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
        return int(float(v)) if v not in (None, "", "--") else 0
    except (TypeError, ValueError):
        return 0


def _safe_float(v):
    try:
        return float(v) if v not in (None, "", "--") else 0.0
    except (TypeError, ValueError):
        return 0.0


# ── Parse asTree=true JSON into user rows ─────────────────────────────────────
def extract_rows_from_tree(data, label, first_campaign=False):
    rows = []
    if not isinstance(data, dict):
        return rows

    col_defs_raw   = data.get("columnDefs", [])
    group_defs_raw = data.get("groupDefs", [])
    groups_raw     = data.get("groups", data.get("children", {}))

    col_names   = _extract_col_names(col_defs_raw)
    group_names = _extract_col_names(group_defs_raw)
    col_map     = {name: f"col{i}" for i, name in enumerate(col_names)}

    if first_campaign:
        print(f"  [{label}] DIAG groupDefs={group_names} colNames={col_names[:8]}")
        print(f"  [{label}] DIAG top-level keys: {list(data.keys())}")
        print(f"  [{label}] DIAG groups type={type(groups_raw).__name__} len={len(groups_raw) if hasattr(groups_raw, '__len__') else '?'}")

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
        return rows

    # Detect nested structure (group0=date, group1=user)
    sample_keys    = [k for k in list(groups.keys())[:3] if k not in ("total", "--", "")]
    date_pattern   = re.compile(r"^\d{4}-\d{2}-\d{2}$")
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
            inner_names   = _extract_col_names(inner_col_raw)
            inner_map     = {name: f"col{i}" for i, name in enumerate(inner_names)}

            for user_key, stats in inner_groups.items():
                if user_key in ("total", "--", "") or not isinstance(stats, dict):
                    continue
                if user_key not in user_agg:
                    user_agg[user_key] = {"name": user_key, "completed": 0, "success": 0, "workTime": 0, "declines": 0}
                agg = user_agg[user_key]
                agg["completed"] += _safe_int(_get_col(stats, inner_map, "completed", "count"))
                agg["success"]   += _safe_int(_get_col(stats, inner_map, "success"))
                agg["workTime"]  += _safe_int(_get_col(stats, inner_map, "workTime"))

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
            sr        = _safe_float(_get_col(stats, col_map, "successRate", "connectRate", "success_rate", default=0))
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
    return rows


# ── Parse a raw row into a dashboard agent dict ───────────────────────────────
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

    work_hrs = work_secs / 3600.0 if work_secs > 0 else 0
    cph = round(calls / work_hrs, 1) if work_hrs > 0 else 0
    sr  = _safe_float(row.get("successRate", 0))
    if sr == 0 and calls > 0:
        sr = round(success / calls * 100, 1)

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


# ── Fetch stats for one campaign ──────────────────────────────────────────────
def fetch_report(campaign, index, total):
    cid      = campaign.get("id", "")
    token    = campaign.get("token", "")
    label    = f"{index + 1}/{total} {cid}"
    is_first = (index == 0)

    if not cid or not token:
        return []

    base        = f"https://api.dialfire.com/api/campaigns/{cid}"
    base_params = {"access_token": token, "timespan": f"0-{DAYS_BACK}day"}

    # Strategy 1: editsDef_v2 group by user
    data1 = fetch_json_report(
        f"{base}/reports/editsDef_v2/report/{LOCALE}",
        {**base_params, "group0": "user",
         "column0": "completed", "column1": "success", "column2": "successRate",
         "column3": "workTime", "column4": "success_p_h", "column5": "completed_p_h"},
        label, "editsDef_v2/report[user]"
    )
    if data1 is None:
        return []
    if isinstance(data1, dict) and data1:
        rows = extract_rows_from_tree(data1, label, is_first)
        if rows:
            return rows

    # Strategy 2: dialerStat group by user (has declines)
    data2 = fetch_json_report(
        f"{base}/reports/dialerStat/report/{LOCALE}",
        {**base_params, "group0": "user",
         "column0": "count", "column1": "connects",
         "column2": "answeringmachines", "column3": "norespons", "column4": "connectRate"},
        label, "dialerStat/report[user]"
    )
    if data2 is None:
        return []
    if isinstance(data2, dict) and data2:
        rows = extract_rows_from_tree(data2, label, False)
        if rows:
            return rows

    # Strategy 3: editsDef_v2 group by date+user (nested)
    data3 = fetch_json_report(
        f"{base}/reports/editsDef_v2/report/{LOCALE}",
        {**base_params, "group0": "date", "group1": "user",
         "column0": "completed", "column1": "success",
         "column2": "successRate", "column3": "workTime"},
        label, "editsDef_v2/report[date,user]"
    )
    if data3 is None:
        return []
    if isinstance(data3, dict) and data3:
        rows = extract_rows_from_tree(data3, label, False)
        if rows:
            return rows

    return []


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=== DialFire Weekly Fetch ===")
    print(f"Period : {DATE_FROM} to {DATE_TO}  ({DAYS_BACK} days, asTree=true JSON)")
    print(f"Tenant : {'***' if TENANT_ID else '(not set)'}")

    # 1. Build campaign list
    campaigns = get_campaigns_from_env()
    if campaigns:
        print(f"Using {len(campaigns)} campaign(s) from CAMPAIGN_n_ID/TOKEN secrets")
    else:
        print("No CAMPAIGN_n_ID/TOKEN secrets found — falling back to tenant discovery...")
        campaigns = get_campaigns_from_tenant()
        if not campaigns:
            raise RuntimeError(
                "No campaigns found. Set CAMPAIGN_1_ID + CAMPAIGN_1_TOKEN secrets, "
                "or set DIALFIRE_TENANT_ID + DIALFIRE_TENANT_TOKEN."
            )

    total = len(campaigns)
    print(f"Active campaigns: {total}\n")

    # 2. Fetch per-campaign stats
    all_rows = []
    for i, campaign in enumerate(campaigns):
        rows = fetch_report(campaign, i, total)
        all_rows.extend(rows)

    rows_with_calls = [r for r in all_rows if _safe_int(r.get("completed", r.get("calls", 0))) > 0]
    print(f"\nRaw agent rows collected: {len(all_rows)}")
    print(f"Raw agent rows with calls > 0: {len(rows_with_calls)}")

    # 3. Merge rows by agent name
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
            ex["declines"] += parsed["declines"]
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
            print(f"  {a['name']:25s} calls={a['calls']:4d}  success={a['success']:4d}  declines={a['declines']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")
    if fancy_agents:
        print("Fancy Callers (top 15):")
        for a in fancy_agents[:15]:
            print(f"  {a['name']:25s} calls={a['calls']:4d}  success={a['success']:4d}  declines={a['declines']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")

    # 4. Write weekly_data.json
    weekly_data = {
        "generated":   now_utc.isoformat(),
        "periodStart": DATE_FROM,
        "periodEnd":   DATE_TO,
        "rm":          rm_agents,
        "fancy":       fancy_agents,
    }
    data_dir  = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data"))
    os.makedirs(data_dir, exist_ok=True)
    data_path = os.path.join(data_dir, "weekly_data.json")
    with open(data_path, "w") as f:
        json.dump(weekly_data, f, indent=2)
    print(f"Saved weekly_data.json  (rm={len(rm_agents)}, fancy={len(fancy_agents)})")

    # 5. Update history.json
    hist_path = os.path.join(data_dir, "history.json")
    history = []
    if os.path.exists(hist_path):
        try:
            raw_hist = json.load(open(hist_path))
            if isinstance(raw_hist, list):
                history = [h for h in raw_hist if isinstance(h, dict) and "weekStart" in h]
        except Exception:
            history = []

    week_entry = {
        "weekStart": DATE_FROM,
        "weekEnd":   DATE_TO,
        "rm":        len(rm_agents),
        "fancy":     len(fancy_agents),
    }
    history = [h for h in history if h.get("weekStart") != DATE_FROM]
    history.append(week_entry)
    history.sort(key=lambda h: h.get("weekStart", ""), reverse=True)
    history = history[:52]

    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Updated history.json  ({len(history)} weeks)")


if __name__ == "__main__":
    main()
