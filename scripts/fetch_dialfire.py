"""
DialFire Campaign Stats -> weekly_data.json fetcher
====================================================
Fetches agent stats from DialFire API using per-campaign tokens.

Leads and email counts come from the Dialfire editsDef_v2 report
grouped by hs_lead_status (outer) and user (inner).

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
            print(f"  [{label}] {tag} -> 403")
            return None
        print(f"  [{label}] {tag} -> HTTP {r.status_code}")
        return []
    except requests.exceptions.Timeout:
        print(f"  [{label}] {tag} -> request timed out")
        return []
    except Exception as e:
        print(f"  [{label}] {tag} -> error: {e}")
        return []


# -- Column name helper -------------------------------------------------------
def _col_names(col_defs):
    names = []
    if isinstance(col_defs, list):
        for cd in col_defs:
            if isinstance(cd, dict):
                names.append(cd.get("name") or cd.get("id") or cd.get("key") or "")
            elif isinstance(cd, str):
                names.append(cd)
    return names


# -- Extract rows from editsDef_v2 dict response ------------------------------
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
            name = str(item.get("user") or item.get("name") or
                       item.get("username") or item.get("agent") or "").strip()
            if not name:
                return None
            d = {"name": name}
            d.update({k: v for k, v in item.items()
                      if k not in ("user", "name", "username", "agent")})
            return d
        return None

    rows = []
    if isinstance(groups_raw, list):
        for item in groups_raw:
            row = _parse_group_item(item, grp_names)
            if row:
                rows.append(row)
    elif isinstance(groups_raw, dict):
        inner_col_defs = groups_raw.get("columnDefs", col_defs)
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


# -- Fetch lead/email counts --------------------------------------------------
def fetch_lead_counts(cid, token, ts, label):
    """
    Fetch hs_lead_status counts per agent.
    editsDef_v2 with group0=hs_lead_status, group1=user needs extended polling.
    Returns: {agent_name: {"seller": N, "rental": N, "email": N}}
    """
    result = {}
    url = f"{API_BASE}/api/campaigns/{cid}/reports/editsDef_v2/report/{LOCALE}"

    # Approach 1: group0=hs_lead_status, group1=user with EXTENDED polling
    # Dialfire returns HTTP 202 and requires polling; hs_lead_status grouping is slower
    params1 = {
        "access_token": token,
        "asTree": "true",
        "timespan": ts,
        "group0": "hs_lead_status",
        "group1": "user",
        "column0": "completed",
    }
    data1 = fetch_json(url, params1, label,
                       "leads: hs_lead_status>user (extended poll)",
                       timeout=30, max_polls=30)  # Up to 30*3s=90s polling
    if data1 is None:
        print(f"  [{label}] leads: 403")
        return result
    if isinstance(data1, dict):
        groups1 = data1.get("groups", [])
        print(f"  [{label}] leads: ap1 groups={len(groups1) if isinstance(groups1,list) else type(groups1).__name__}")
        if isinstance(groups1, list) and len(groups1) > 0:
            first = groups1[0]
            if isinstance(first, dict):
                inner_check = first.get("groups", first.get("children", None))
                print(f"  [{label}] leads: ap1 first keys={list(first.keys())} inner={type(inner_check).__name__ if inner_check is not None else 'NONE'}")
                if isinstance(inner_check, list):
                    # We have nested data!
                    for status_item in groups1:
                        if not isinstance(status_item, dict):
                            continue
                        status_val = str(status_item.get("value", status_item.get("name", ""))).strip().upper()
                        if status_val in SELLER_STATUSES:
                            bucket = "seller"
                        elif status_val in RENTAL_STATUSES:
                            bucket = "rental"
                        elif status_val in EMAIL_STATUSES:
                            bucket = "email"
                        else:
                            continue
                        inner = status_item.get("groups", status_item.get("children", []))
                        for user_item in (inner if isinstance(inner, list) else []):
                            if not isinstance(user_item, dict):
                                continue
                            agent_name = str(user_item.get("value", user_item.get("name", ""))).strip()
                            if not agent_name or agent_name == "\u2014":
                                continue
                            cols = user_item.get("columns", [])
                            count = 1
                            if isinstance(cols, list) and cols:
                                try:
                                    count = int(round(float(cols[0])))
                                except Exception:
                                    pass
                            if agent_name not in result:
                                result[agent_name] = {"seller": 0, "rental": 0, "email": 0}
                            result[agent_name][bucket] += count
                    if result:
                        print(f"  [{label}] leads: ap1 SUCCESS -> {result}")
                        return result
                    print(f"  [{label}] leads: ap1 nested groups but no matching status values")
                else:
                    # Flat response - log the status values we see
                    print(f"  [{label}] leads: ap1 flat - status values: {[g.get('value','') for g in groups1 if isinstance(g,dict)]}")

    # Approach 2: group0=user, group1=hs_lead_status (extended poll)
    params2 = {
        "access_token": token,
        "asTree": "true",
        "timespan": ts,
        "group0": "user",
        "group1": "hs_lead_status",
        "column0": "completed",
    }
    data2 = fetch_json(url, params2, label,
                       "leads: user>hs_lead_status (extended poll)",
                       timeout=30, max_polls=30)
    if data2 is None:
        print(f"  [{label}] leads: ap2 403")
        return result
    if isinstance(data2, dict):
        groups2 = data2.get("groups", [])
        print(f"  [{label}] leads: ap2 groups={len(groups2) if isinstance(groups2,list) else type(groups2).__name__}")
        if isinstance(groups2, list) and len(groups2) > 0:
            for agent_item in groups2:
                if not isinstance(agent_item, dict):
                    continue
                agent_name = str(agent_item.get("value", agent_item.get("name", ""))).strip()
                if not agent_name or agent_name == "\u2014":
                    continue
                inner = agent_item.get("groups", agent_item.get("children", []))
                if not isinstance(inner, list) or not inner:
                    continue
                for status_item in inner:
                    if not isinstance(status_item, dict):
                        continue
                    status_val = str(status_item.get("value", status_item.get("name", ""))).strip().upper()
                    cols = status_item.get("columns", [])
                    count = 1
                    if isinstance(cols, list) and cols:
                        try:
                            count = int(round(float(cols[0])))
                        except Exception:
                            pass
                    if agent_name not in result:
                        result[agent_name] = {"seller": 0, "rental": 0, "email": 0}
                    if status_val in SELLER_STATUSES:
                        result[agent_name]["seller"] += count
                    elif status_val in RENTAL_STATUSES:
                        result[agent_name]["rental"] += count
                    elif status_val in EMAIL_STATUSES:
                        result[agent_name]["email"] += count
            if result:
                print(f"  [{label}] leads: ap2 SUCCESS -> {result}")
                return result
            print(f"  [{label}] leads: ap2 no nested status data")

    print(f"  [{label}] leads: all approaches failed - leads will be 0")
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
    if work_h > 1000:
        work_h = round(work_h / 3600, 2)

    sr_raw = row.get("successRate") or row.get("success_rate") or 0
    try:
        sr_float = float(sr_raw)
        if 0.0 <= sr_float <= 1.0:
            sr = round(sr_float * 100, 1)
        else:
            sr = round(sr_float, 1)
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
            if grp_len > 0:
                rows = extract_rows(data, label)
                if rows:
                    print(f"  [{label}] SUCCESS with ts={ts}")
                    lead_counts = fetch_lead_counts(cid, token, ts, label)
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
    print(f"\nWritten to {os.path.abspath(out_path)}")

    hist_path = os.path.join(os.path.dirname(__file__), "..", "data", "history.json")
    try:
        with open(hist_path) as f:
            history = json.load(f)
        if not isinstance(history, list):
            history = []
    except Exception:
        history = []

    entry = {
        "generated": now_utc.isoformat(),
        "week":      week_str,
        "weekStart": str(period_start),
        "weekEnd":   str(period_end),
        "rm":        rm_agents,
        "fancy":     fancy_agents,
    }

    history = [h for h in history if h.get("weekStart") != str(period_start)]
    history.insert(0, entry)
    history = history[:52]

    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"History updated: {len(history)} weeks")


if __name__ == "__main__":
    main()
