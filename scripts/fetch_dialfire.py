"""
DialFire Campaign Stats -> weekly_data.json fetcher
====================================================
Fetches agent stats from DialFire API using per-campaign tokens.

Timespan format (discovered via testing):
  "0-0day"  = today only
  "7-0day"  = last 7 days (positive N, no minus sign)
  "14-0day" = last 14 days

API response format (asTree):
  groups is a list of {"value": "AgentName", "columns": [v0, v1, ...]}
  where column order matches columnDefs order.
"""

import os, json, re, time, datetime, pytz
import requests

# -- Config -------------------------------------------------------------------
LOCALE = "en_US"
DAYS_BACK = 7
TIMEZONE = pytz.timezone("Africa/Johannesburg")
API_BASE = "https://api.dialfire.com"

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


# -- Poll helper --------------------------------------------------------------
def fetch_json(url, params, label, tag, max_polls=8, poll_interval=3):
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 403:
            print(f"  [{label}] {tag} -> HTTP 403 (skip)")
            return None
        if r.status_code == 404:
            print(f"  [{label}] {tag} -> HTTP 404 (skip)")
            return None
        if r.status_code == 202:
            print(f"  [{label}] {tag} -> HTTP 202 (polling...)")
            for _ in range(max_polls):
                time.sleep(poll_interval)
                r2 = requests.get(url, params=params, timeout=30)
                if r2.status_code == 200:
                    print(f"  [{label}] {tag} -> HTTP 200 (after poll)")
                    try:
                        return r2.json()
                    except Exception as e:
                        print(f"  [{label}] JSON error after poll: {e}")
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
        print(f"  [{label}] {tag} -> HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  [{label}] {tag} -> Exception: {e}")
        return []


# -- Helpers ------------------------------------------------------------------
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

def _col_names(defs):
    result = []
    for item in defs:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            result.append(item.get("name", ""))
        else:
            result.append(str(item))
    return result


# -- Parse asTree JSON into rows ----------------------------------------------
def extract_rows(data, label):
    rows = []
    if not isinstance(data, dict):
        print(f"  [{label}] DIAG: not a dict ({type(data).__name__})")
        return rows

    col_defs = data.get("columnDefs", [])
    grp_defs = data.get("groupDefs", [])
    groups_raw = data.get("groups", data.get("children", []))

    col_names = _col_names(col_defs)
    grp_names = _col_names(grp_defs)

    grp_len = len(groups_raw) if hasattr(groups_raw, "__len__") else "?"
    print(f"  [{label}] DIAG grpDefs={grp_names} cols={col_names} groups={type(groups_raw).__name__}[{grp_len}]")

    if not groups_raw:
        return rows

    def _parse_group_item(item, col_names_list):
        if not isinstance(item, dict):
            return None
        # New API format: {"value": "AgentName", "columns": [v0, v1, ...]}
        if "columns" in item and isinstance(item["columns"], list):
            name = str(item.get("value", item.get("name", item.get("user", "")))).strip()
            cols = item["columns"]
            d = {"name": name}
            for i, cn in enumerate(col_names_list):
                d[cn] = cols[i] if i < len(cols) else 0
            return d
        # Old format: {"user": "AgentName", "col0": v0, ...}
        name = str(item.get("user", item.get("name", item.get("value", "")))).strip()
        if not name:
            return None
        d = {"name": name}
        for i, cn in enumerate(col_names_list):
            if cn in item:
                d[cn] = item[cn]
            elif f"col{i}" in item:
                d[cn] = item[f"col{i}"]
            else:
                d[cn] = 0
        return d

    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    if isinstance(groups_raw, list):
        sample_values = []
        for item in groups_raw[:3]:
            if isinstance(item, dict):
                v = str(item.get("value", item.get("name", item.get("user", ""))))
                if v:
                    sample_values.append(v)

        is_nested = bool(sample_values) and all(date_re.match(v) for v in sample_values) and len(grp_names) > 1

        if is_nested:
            agg = {}
            for date_item in groups_raw:
                if not isinstance(date_item, dict):
                    continue
                date_val = str(date_item.get("value", date_item.get("name", "")))
                if not date_re.match(date_val):
                    continue
                inner_raw = date_item.get("groups", date_item.get("children", []))
                inner_cols = _col_names(date_item.get("columnDefs", col_defs))
                if not isinstance(inner_raw, list):
                    continue
                for inner_item in inner_raw:
                    parsed = _parse_group_item(inner_item, inner_cols if inner_cols else col_names)
                    if not parsed:
                        continue
                    uk = parsed["name"]
                    if not uk or uk.lower() in ("total", "--", "grand total"):
                        continue
                    if uk not in agg:
                        agg[uk] = {"name": uk, "completed": 0, "success": 0, "workTime": 0, "declines": 0, "successRate": 0.0}
                    a = agg[uk]
                    a["completed"] += _safe_int(parsed.get("completed", parsed.get("count", 0)))
                    a["success"] += _safe_int(parsed.get("success", parsed.get("connects", 0)))
                    a["workTime"] += _safe_int(parsed.get("workTime", 0))
                    a["declines"] += (
                        _safe_int(parsed.get("norespons", parsed.get("noResponse", 0))) +
                        _safe_int(parsed.get("answeringmachines", parsed.get("answeringMachines", 0)))
                    )
            for a in agg.values():
                a["successRate"] = round(a["success"] / a["completed"] * 100, 1) if a["completed"] > 0 else 0.0
            rows = list(agg.values())
        else:
            for item in groups_raw:
                parsed = _parse_group_item(item, col_names)
                if not parsed:
                    continue
                name = parsed["name"]
                if not name or name.lower() in ("total", "--", "grand total"):
                    continue
                rows.append(parsed)

    elif isinstance(groups_raw, dict):
        for key, stats in groups_raw.items():
            if key in ("total", "--", "") or not isinstance(stats, dict):
                continue
            parsed = _parse_group_item({**stats, "name": key}, col_names)
            if parsed:
                rows.append(parsed)

    print(f"  [{label}] extracted {len(rows)} rows")
    if rows:
        print(f"  [{label}] sample row: {rows[0]}")
    return rows


# -- Parse row into dashboard agent -------------------------------------------
def parse_row(row):
    if not isinstance(row, dict):
        return None
    name = str(row.get("name", row.get("user", ""))).strip()
    if not name or name.lower() in ("total", "--", "grand total"):
        return None

    calls = _safe_int(row.get("completed", row.get("count", row.get("calls", 0))))
    success = _safe_int(row.get("success", row.get("connects", 0)))
    declines = _safe_int(row.get("declines", 0))
    work_val = row.get("workTime", 0)

    if calls == 0:
        return None

    work_raw = _safe_float(work_val)
    # workTime > 24 means seconds, else hours
    work_hrs = work_raw / 3600.0 if work_raw > 24 else work_raw

    cph = round(calls / work_hrs, 1) if work_hrs > 0 else 0.0

    sr = row.get("successRate")
    if sr is None or sr == "":
        sr = round(success / calls * 100, 1) if calls > 0 else 0.0
    else:
        sr = _safe_float(sr)
        # successRate stored as fraction (0-1) -> convert to percent
        if 0.0 < sr <= 1.0 and success > 0 and calls > 0:
            computed = success / calls
            if abs(sr - computed) < 0.01:
                sr = round(sr * 100, 1)

    return {
        "name": name, "calls": calls, "success": success, "declines": declines,
        "cph": cph, "successRate": sr, "workHours": round(work_hrs, 2),
        "meetsTarget": cph >= BENCHMARKS["cph"],
    }


# -- Fetch one campaign -------------------------------------------------------
def fetch_campaign(cid, token, index, total):
    label = f"{index + 1}/{total} {cid}"
    base = f"{API_BASE}/api/campaigns/{cid}"

    timespans = ["0-0day", f"{DAYS_BACK}-0day", "14-0day", "7-0day", "30-0day"]

    for ts in timespans:
        params = {"access_token": token, "asTree": "true", "timespan": ts,
                  "group0": "user", "column0": "completed", "column1": "success",
                  "column2": "successRate", "column3": "workTime"}
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
                    return rows

    for ts in timespans:
        params = {"access_token": token, "asTree": "true", "timespan": ts,
                  "group0": "user", "column0": "count", "column1": "connects",
                  "column2": "answeringmachines", "column3": "norespons", "column4": "connectRate"}
        data = fetch_json(f"{base}/reports/dialerStat/report/{LOCALE}", params,
                          label, f"dialerStat ts={ts}")
        if data is None:
            return []
        if isinstance(data, dict):
            grp = data.get("groups", [])
            grp_len = len(grp) if hasattr(grp, "__len__") else 0
            print(f"  [{label}] dialerStat ts={ts} groups={type(grp).__name__}[{grp_len}]")
            if grp_len > 0:
                rows = extract_rows(data, label)
                if rows:
                    print(f"  [{label}] SUCCESS dialerStat ts={ts}")
                    return rows

    print(f"  [{label}] No data found across all timespans")
    return []


# -- Main ---------------------------------------------------------------------
def main():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_sast = now_utc.astimezone(TIMEZONE)
    period_end = now_sast.date()
    period_start = period_end - datetime.timedelta(days=DAYS_BACK)

    print("=== DialFire Weekly Fetch ===")
    print(f"Period : {period_start} to {period_end} ({DAYS_BACK} days)")

    campaigns = []

    ch_id = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
    ch_tok = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()
    if ch_id and ch_tok:
        campaigns.append({"id": ch_id, "token": ch_tok, "label": "CLIENTHUB"})
        print(f"  CLIENTHUB campaign: {ch_id}")
    elif ch_id:
        print(f"  CLIENTHUB campaign: {ch_id} (NO TOKEN)")

    i = 1
    while True:
        cid = os.environ.get(f"CAMPAIGN_{i}_ID", "").strip()
        ctok = os.environ.get(f"CAMPAIGN_{i}_TOKEN", "").strip()
        if not cid:
            break
        if ctok:
            campaigns.append({"id": cid, "token": ctok, "label": f"CAMP{i}"})
            print(f"  Campaign {i}: {cid}")
        else:
            print(f"  Campaign {i}: {cid} (NO TOKEN - skipping)")
        i += 1

    if not campaigns:
        print("No campaigns configured.")
        return

    print(f"Total campaigns: {len(campaigns)}")
    print()

    all_rows = []
    for idx, c in enumerate(campaigns):
        rows = fetch_campaign(c["id"], c["token"], idx, len(campaigns))
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
            ex["calls"] += agent["calls"]
            ex["success"] += agent["success"]
            ex["declines"] += agent["declines"]
            ex["workHours"] = round(ex["workHours"] + agent["workHours"], 2)
            ex["cph"] = round(ex["calls"] / ex["workHours"], 1) if ex["workHours"] > 0 else 0.0
            ex["successRate"] = round(ex["success"] / ex["calls"] * 100, 1) if ex["calls"] > 0 else 0.0
            ex["meetsTarget"] = ex["cph"] >= BENCHMARKS["cph"]
        else:
            merged[name] = agent

    print(f"Unique agents: {len(merged)}")

    rm_agents, fancy_agents = [], []
    for name, agent in sorted(merged.items()):
        (rm_agents if name in RM_NAMES else fancy_agents).append(agent)

    rm_agents.sort(key=lambda x: x["calls"], reverse=True)
    fancy_agents.sort(key=lambda x: x["calls"], reverse=True)

    print(f"RM: {len(rm_agents)} | Fancy: {len(fancy_agents)}")
    for a in rm_agents:
        print(f"  RM    {a['name']:25s} calls={a['calls']:4d} success={a['success']:3d} declines={a['declines']:3d} cph={a['cph']:5.1f}")
    for a in fancy_agents[:15]:
        print(f"  FANCY {a['name']:25s} calls={a['calls']:4d} success={a['success']:3d} declines={a['declines']:3d} cph={a['cph']:5.1f}")

    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    weekly = {"generated": now_utc.isoformat(), "periodStart": str(period_start),
              "periodEnd": str(period_end), "rm": rm_agents, "fancy": fancy_agents}
    with open(os.path.join(data_dir, "weekly_data.json"), "w") as f:
        json.dump(weekly, f, indent=2)
    print(f"Saved weekly_data.json (rm={len(rm_agents)}, fancy={len(fancy_agents)})")

    history_path = os.path.join(data_dir, "history.json")
    try:
        with open(history_path) as f:
            raw = json.load(f)
    except Exception:
        raw = []
    history = [h for h in raw if isinstance(h, dict) and "weekStart" in h]
    week_key = str(period_start)
    history = [h for h in history if h.get("weekStart") != week_key]
    history.append({"weekStart": week_key, "weekEnd": str(period_end),
                    "generated": now_utc.isoformat(), "rm": rm_agents, "fancy": fancy_agents})
    history.sort(key=lambda x: x["weekStart"])
    history = history[-12:]
    with open(history_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Updated history.json ({len(history)} weeks)")


if __name__ == "__main__":
    main()
