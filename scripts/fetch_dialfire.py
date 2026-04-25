"""
DialFire Campaign Stats -> weekly_data.json fetcher
====================================================
Fetches agent stats from DialFire API using per-campaign tokens.

Leads and email counts come from the Dialfire editsDef_v2 report
grouped by Lead_Status (outer) and user (inner).

Lead_Status mapping:
  seller : LEAD (Seller Lead, On the Market, Wants a Valuation)
  rental : RENTAL_LEAD
  email  : GOT_EMAIL
"""

import os, json, re, time, datetime, pytz
import requests

# -- Config -------------------------------------------------------------------
LOCALE = "en_US"
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


# -- Date helpers -------------------------------------------------------------
def get_current_week_bounds(now_sast):
    today = now_sast.date()
    monday = today - datetime.timedelta(days=today.weekday())
    friday = monday + datetime.timedelta(days=4)
    return monday, friday


def build_timespan(period_start, period_end, now_sast):
    today = now_sast.date()
    weekday = today.weekday()  # 0=Mon, 5=Sat, 6=Sun
    # DialFire editsDef_v2 only returns HTTP 200 for timespans ending today.
    # On weekends, shift the reference date back to last Friday.
    if weekday == 5:    # Saturday: yesterday = Friday
        days_to_end = 1
    elif weekday == 6:  # Sunday: 2 days ago = Friday
        days_to_end = 2
    else:               # Weekday: today
        days_to_end = 0
    days_to_start = (today - period_start).days + days_to_end
    return f"{days_to_start}-{days_to_end}day"


# -- Poll helper --------------------------------------------------------------
def fetch_json(url, params, label, tag, timeout=30, max_polls=8, headers=None):
    try:
        r = requests.get(url, params=params, timeout=timeout,
                         headers=headers or {})
        if r.status_code == 202:
            poll_url = None
            try:
                body202 = r.json()
                poll_url = body202.get("url") or body202.get("statusUrl") or r.headers.get("Location")
            except Exception:
                poll_url = r.headers.get("Location")
            if not poll_url:
                loc_hdr = dict(r.headers).get("Location", dict(r.headers).get("location", ""))
                print(f"  [{label}] {tag}: HTTP 202 - no poll URL (Location={loc_hdr!r}, body={r.text[:100]!r})")
                return {}
            for attempt in range(max_polls):
                time.sleep(3)
                pr = requests.get(poll_url, timeout=timeout,
                                  headers=headers or {})
                if pr.status_code == 200:
                    try:
                        return pr.json()
                    except Exception:
                        return None
                elif pr.status_code != 202:
                    print(f"  [{label}] {tag} poll {attempt+1}: HTTP {pr.status_code}")
                    break
            print(f"  [{label}] {tag} polling timed out after {max_polls} attempts")
            return {}
        if r.status_code == 403:
            print(f"  [{label}] {tag}: HTTP 403 - invalid token")
            return None
        if r.status_code != 200:
            print(f"  [{label}] {tag}: HTTP {r.status_code}")
            return {}
        try:
            return r.json()
        except Exception as e:
            print(f"  [{label}] {tag}: JSON parse error: {e}")
            return {}
    except Exception as e:
        print(f"  [{label}] {tag}: exception: {e}")
        return {}


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
            name = str(item["value"]).strip()
            if not name or name in ("-", "\u2014", "\u2013"):
                return None
            row = {"name": name}
            for i, cname in enumerate(cn):
                if i < len(cols):
                    row[cname] = cols[i]
            return row
        return None

    rows = []
    if isinstance(groups_raw, list):
        for item in groups_raw:
            row = _parse_group_item(item, grp_names)
            if row:
                rows.append(row)
                continue
            if isinstance(item, dict) and "groups" in item:
                inner_col_defs = item.get("columnDefs", col_defs)
                cn = _cn(inner_col_defs)
                inner_groups = item.get("groups", [])
                if isinstance(inner_groups, list):
                    for sub in inner_groups:
                        row = _parse_group_item(sub, cn)
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
    Fetch Lead_Status counts per agent.
    Approach 1: contacts/filter POST with Bearer token (returns contact records with Lead_Status per agent).
    Approach 2: editsDef_v2 group0=Lead_Status, group1=user.
    Approach 3: editsDef_v2 group0=user, group1=Lead_Status.
    Returns: {agent_name: {"seller": N, "rental": N, "email": N}, ...}
    """
    result = {}
    base_url = f"{API_BASE}/api/campaigns/{cid}/reports/editsDef_v2/report/{LOCALE}"
    contacts_url = f"{API_BASE}/api/campaigns/{cid}/contacts/filter"
    headers_bearer = {"Authorization": f"Bearer {token}"}

    # --- Approach 1: contacts/filter POST - get all contacts and count by Lead_Status per agent ---
    try:
        r_cf = requests.post(contacts_url, headers=headers_bearer,
                             json={"fields": ["Lead_Status", "hs_lead_status", "assigned_user",
                                              "$Lead_Status", "$hs_lead_status", "$assigned_user",
                                              "last_edit_user", "last_edit_time"]},
                             timeout=30)
        print(f"  [{label}] contacts/filter: HTTP {r_cf.status_code}")
        if r_cf.status_code == 200:
            contacts = r_cf.json() if r_cf.text else []
            if isinstance(contacts, list) and len(contacts) > 0:
                sample = contacts[0] if isinstance(contacts[0], dict) else {}
                sample_keys = list(sample.keys())[:20]
                print(f"  [{label}] contacts sample keys: {sample_keys}")
                # Try to find Lead_Status and agent fields
                lead_field = None
                agent_field = None
                for fld in ["Lead_Status", "hs_lead_status", "$Lead_Status", "$hs_lead_status"]:
                    if fld in sample:
                        lead_field = fld
                        break
                for fld in ["assigned_user", "$assigned_user", "last_edit_user", "$last_edit_user"]:
                    if fld in sample:
                        agent_field = fld
                        break
                print(f"  [{label}] contacts lead_field={lead_field} agent_field={agent_field} total={len(contacts)}")
                if lead_field and agent_field:
                    for contact in contacts:
                        if not isinstance(contact, dict):
                            continue
                        status_val = str(contact.get(lead_field, "")).strip().upper()
                        agent_name = str(contact.get(agent_field, "")).strip()
                        if not agent_name or agent_name in ("-", "None", ""):
                            continue
                        bucket = None
                        if status_val in {s.upper() for s in SELLER_STATUSES}:
                            bucket = "seller"
                        elif status_val in {s.upper() for s in RENTAL_STATUSES}:
                            bucket = "rental"
                        elif status_val in {s.upper() for s in EMAIL_STATUSES}:
                            bucket = "email"
                        if bucket:
                            if agent_name not in result:
                                result[agent_name] = {"seller": 0, "rental": 0, "email": 0}
                            result[agent_name][bucket] += 1
                    if result:
                        print(f"  [{label}] contacts leads SUCCESS: {result}")
                        return result
                    else:
                        # Show sample values for debugging
                        sample_leads = [str(c.get(lead_field, ""))[:30] for c in contacts[:5] if isinstance(c, dict)]
                        sample_agents = [str(c.get(agent_field, ""))[:20] for c in contacts[:5] if isinstance(c, dict)]
                        print(f"  [{label}] contacts: no matching statuses. Sample leads={sample_leads} agents={sample_agents}")
            elif isinstance(contacts, dict):
                print(f"  [{label}] contacts returned dict with keys: {list(contacts.keys())[:10]}")
                # contacts/filter returns {cursor, hits, _count_, _checked_} - extract hits list
                hits = contacts.get("hits", [])
                if isinstance(hits, list) and len(hits) > 0:
                    contacts = hits  # reuse list-processing logic below
                    sample = contacts[0] if isinstance(contacts[0], dict) else {}
                    sample_keys = list(sample.keys())[:20]
                    print(f"  [{label}] contacts hits sample keys: {sample_keys}")
                    lead_field = None
                    agent_field = None
                    for fld in ["Lead_Status", "hs_lead_status", "$Lead_Status", "$hs_lead_status"]:
                        if fld in sample:
                            lead_field = fld
                            break
                    for fld in ["assigned_user", "$assigned_user", "last_edit_user", "$last_edit_user"]:
                        if fld in sample:
                            agent_field = fld
                            break
                    print(f"  [{label}] hits: lead_field={lead_field} agent_field={agent_field}")
                    if lead_field and agent_field:
                        for c in contacts:
                            if not isinstance(c, dict):
                                continue
                            status_val = str(c.get(lead_field, "") or "").strip().upper()
                            agent_name = str(c.get(agent_field, "") or "").strip()
                            if not agent_name:
                                continue
                            bucket = None
                            if status_val in {s.upper() for s in SELLER_STATUSES}:
                                bucket = "seller"
                            elif status_val in {s.upper() for s in RENTAL_STATUSES}:
                                bucket = "rental"
                            elif status_val in {s.upper() for s in EMAIL_STATUSES}:
                                bucket = "email"
                            if bucket:
                                if agent_name not in result:
                                    result[agent_name] = {"seller": 0, "rental": 0, "email": 0}
                                result[agent_name][bucket] += 1
                        if result:
                            print(f"  [{label}] contacts hits leads SUCCESS: {result}")
                            return result
                        else:
                            sample_leads = [str(c.get(lead_field, ""))[:30] for c in contacts[:5] if isinstance(c, dict)]
                            sample_agents = [str(c.get(agent_field, ""))[:20] for c in contacts[:5] if isinstance(c, dict)]
                            print(f"  [{label}] contacts hits: no matching statuses. Sample leads={sample_leads} agents={sample_agents}")
                    else:
                        all_keys = list(contacts[0].keys()) if contacts else []
                        print(f"  [{label}] contacts hits: cannot find lead/agent fields. All keys={all_keys}")
                else:
                    print(f"  [{label}] contacts hits: empty or not a list. hits type={type(hits).__name__} len={len(hits) if isinstance(hits,list) else 'N/A'}")
        elif r_cf.status_code == 401 or r_cf.status_code == 403:
            print(f"  [{label}] contacts/filter: auth failed ({r_cf.status_code}), trying access_token...")
            # Try with access_token instead of Bearer
            r_cf2 = requests.post(contacts_url,
                                  json={"fields": ["Lead_Status", "hs_lead_status", "assigned_user"]},
                                  params={"access_token": token},
                                  headers={"Content-Type": "application/json"},
                                  timeout=30)
            print(f"  [{label}] contacts/filter (access_token): HTTP {r_cf2.status_code}")
            if r_cf2.status_code == 200:
                contacts2 = r_cf2.json() if r_cf2.text else []
                print(f"  [{label}] contacts (access_token): {len(contacts2) if isinstance(contacts2, list) else type(contacts2).__name__} items")
    except Exception as e:
        print(f"  [{label}] contacts/filter exception: {e}")

    # --- Approach 2: editsDef_v2 group0=Lead_Status, group1=user ---
    params1 = {
        "access_token": token,
        "asTree": "true",
        "timespan": ts,
        "group0": "Lead_Status",
        "group1": "user",
        "column0": "completed",
    }
    data1 = fetch_json(base_url, params1, label,
                       "leads ap2: Lead_Status>user",
                       timeout=30, max_polls=30)
    if data1 is not None and isinstance(data1, dict):
        groups1 = data1.get("groups", [])
        print(f"  [{label}] leads ap2: groups={len(groups1) if isinstance(groups1,list) else type(groups1).__name__}")
        if isinstance(groups1, list) and len(groups1) > 0:
            first = groups1[0]
            if isinstance(first, dict):
                inner = first.get("groups", first.get("children", None))
                print(f"  [{label}] leads ap2 first: value={repr(str(first.get('value',''))[:40])} inner={type(inner).__name__ if inner is not None else 'NONE'}")
                if isinstance(inner, list) and len(inner) > 0:
                    for grp in groups1:
                        status_val = str(grp.get("value", "")).strip().upper()
                        bucket = None
                        if status_val in {s.upper() for s in SELLER_STATUSES}:
                            bucket = "seller"
                        elif status_val in {s.upper() for s in RENTAL_STATUSES}:
                            bucket = "rental"
                        elif status_val in {s.upper() for s in EMAIL_STATUSES}:
                            bucket = "email"
                        if bucket:
                            inner_grps = grp.get("groups", grp.get("children", []))
                            for u in (inner_grps if isinstance(inner_grps, list) else []):
                                if isinstance(u, dict):
                                    agent_name = str(u.get("value", ""))
                                    ucols = u.get("columns", [])
                                    count = 0
                                    if isinstance(ucols, list) and len(ucols) > 0:
                                        try:
                                            count = int(ucols[0]) if ucols[0] not in (None, "", "-") else 0
                                        except (ValueError, TypeError):
                                            pass
                                    if agent_name and agent_name != "-":
                                        if agent_name not in result:
                                            result[agent_name] = {"seller": 0, "rental": 0, "email": 0}
                                        result[agent_name][bucket] += count
                    if result:
                        print(f"  [{label}] leads ap2 SUCCESS: {result}")
                        return result

    # --- Approach 3: editsDef_v2 group0=user, group1=Lead_Status ---
    params2 = {
        "access_token": token,
        "asTree": "true",
        "timespan": ts,
        "group0": "user",
        "group1": "Lead_Status",
        "column0": "completed",
    }
    data2 = fetch_json(base_url, params2, label,
                       "leads ap3: user>Lead_Status",
                       timeout=30, max_polls=30)
    if data2 is not None and isinstance(data2, dict):
        groups2 = data2.get("groups", [])
        print(f"  [{label}] leads ap3: groups={len(groups2) if isinstance(groups2,list) else type(groups2).__name__}")
        if isinstance(groups2, list) and len(groups2) > 0:
            first2 = groups2[0]
            if isinstance(first2, dict):
                inner2 = first2.get("groups", first2.get("children", None))
                print(f"  [{label}] leads ap3 first: value={repr(str(first2.get('value',''))[:30])} inner={type(inner2).__name__ if inner2 is not None else 'NONE'}")
                if isinstance(inner2, list) and len(inner2) > 0:
                    for ugrp in groups2:
                        agent_name = str(ugrp.get("value", ""))
                        inner_grps = ugrp.get("groups", ugrp.get("children", []))
                        if not isinstance(inner_grps, list):
                            continue
                        for sgrp in inner_grps:
                            if not isinstance(sgrp, dict):
                                continue
                            status_val = str(sgrp.get("value", "")).strip().upper()
                            bucket = None
                            if status_val in {s.upper() for s in SELLER_STATUSES}:
                                bucket = "seller"
                            elif status_val in {s.upper() for s in RENTAL_STATUSES}:
                                bucket = "rental"
                            elif status_val in {s.upper() for s in EMAIL_STATUSES}:
                                bucket = "email"
                            if bucket:
                                scols = sgrp.get("columns", [])
                                count = 0
                                if isinstance(scols, list) and len(scols) > 0:
                                    try:
                                        count = int(scols[0]) if scols[0] not in (None, "", "-") else 0
                                    except (ValueError, TypeError):
                                        pass
                                if agent_name and agent_name != "-":
                                    if agent_name not in result:
                                        result[agent_name] = {"seller": 0, "rental": 0, "email": 0}
                                    result[agent_name][bucket] += count
                    if result:
                        print(f"  [{label}] leads ap3 SUCCESS: {result}")
                        return result

    print(f"  [{label}] leads: all approaches failed - leads will be 0")
    return result


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



def fetch_campaign(cid, token, index, total, period_start, period_end, ts):
    label = f"{index + 1}/{total} {cid}"
    base = f"{API_BASE}/api/campaigns/{cid}"

    seen = set()
    unique_ts = []
    for t in [ts, "0-0day", "7-0day", "14-0day", "30-0day"]:
        if t not in seen:
            seen.add(t)
            unique_ts.append(t)

    for cur_ts in unique_ts:
        params = {
            "access_token": token,
            "asTree": "true",
            "timespan": cur_ts,
            "group0": "user",
            "column0": "completed",
            "column1": "success",
            "column2": "successRate",
            "column3": "workTime",
        }
        data = fetch_json(f"{base}/reports/editsDef_v2/report/{LOCALE}", params,
                          label, f"editsDef_v2 ts={cur_ts}")
        if data is None:
            print(f"  [{label}] 403 - token invalid, skipping campaign")
            return []
        if isinstance(data, dict):
            grp = data.get("groups", [])
            grp_len = len(grp) if hasattr(grp, "__len__") else 0
            if grp_len > 0:
                rows = extract_rows(data, label)
                if rows:
                    print(f"  [{label}] SUCCESS with ts={cur_ts}")
                    lead_counts = fetch_lead_counts(cid, token, cur_ts, label)
                    for row in rows:
                        name = row.get("name", "")
                        if name in lead_counts:
                            row["seller"] = lead_counts[name]["seller"]
                            row["rental"] = lead_counts[name]["rental"]
                            row["email"]  = lead_counts[name]["email"]
                    return rows
        else:
            print(f"  [{label}] ts={cur_ts} got non-dict: {type(data).__name__}")

    print(f"  [{label}] all timespans failed")
    return []


# -- Main ---------------------------------------------------------------------
def main():
    now_utc  = datetime.datetime.now(datetime.timezone.utc)
    now_sast = now_utc.astimezone(TIMEZONE)

    period_start, period_end = get_current_week_bounds(now_sast)
    ts = build_timespan(period_start, period_end, now_sast)

    print("=== DialFire Weekly Fetch ===")
    print(f"Period : {period_start} (Mon) to {period_end} (Fri)")
    print(f"Timespan: {ts}")

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

    leg_id  = os.environ.get("DIALFIRE_CAMPAIGN_ID", "").strip()
    leg_tok = os.environ.get("DIALFIRE_CAMPAIGN_TOKEN", "").strip()
    if leg_id and leg_tok:
        if not any(c["id"] == leg_id for c in campaigns):
            campaigns.append({"id": leg_id, "token": leg_tok, "label": "LEGACY"})
            print(f"  Legacy campaign: {leg_id}")

    if not campaigns:
        print("No campaigns configured.")
        return

    print(f"Total campaigns: {len(campaigns)}")
    print()

    all_rows = []
    for idx, c in enumerate(campaigns):
        rows = fetch_campaign(c["id"], c["token"], idx, len(campaigns),
                              period_start, period_end, ts)
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

    rm_agents    = sorted([a for a in agents if a["name"] in RM_NAMES],     key=lambda x: -x["calls"])
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
