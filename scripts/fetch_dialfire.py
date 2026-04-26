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

# RM classification is now campaign-based:
# agents calling campaign_clienthub are RM; all others are Fancy.

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


# -- Fetch actual campaign name from Dialfire API -----------------------------
def fetch_campaign_name(cid, token):
    """
    Fetch the human-readable campaign name for a given campaign ID.
    Tries GET /api/campaigns/{cid} with access_token param.
    Returns the name string, or cid as fallback.
    """
    url = f"{API_BASE}/api/campaigns/{cid}"
    try:
        r = requests.get(url, params={"access_token": token}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            name = (data.get("name") or data.get("title") or data.get("label") or "").strip()
            if name:
                print(f"  [campaign {cid}] name: {name!r}")
                return name
        print(f"  [campaign {cid}] name fetch HTTP {r.status_code}, using id as fallback")
    except Exception as e:
        print(f"  [campaign {cid}] name fetch error: {e}")
    return cid


# -- Fetch lead/email counts --------------------------------------------------
def fetch_lead_counts(cid, token, ts, label):
    """
    Fetch Lead_Status counts per agent.
    Approach 1: contacts/filter POST - if hits contain full fields, parse them.
    Approach 2: editsDef_v2 group0=Lead_Status, group1=user (fastest, works in practice).
    Approach 3: editsDef_v2 group0=user, group1=Lead_Status.
    Returns: {agent_name: {"seller": N, "rental": N, "email": N}, ...}
    """
    result = {}
    base_url     = f"{API_BASE}/api/campaigns/{cid}/reports/editsDef_v2/report/{LOCALE}"
    contacts_url = f"{API_BASE}/api/campaigns/{cid}/contacts/filter"
    bearer_hdr   = {"Authorization": f"Bearer {token}"}

    # --- Approach 1: contacts/filter - only proceed if hits have full field data ---
    try:
        r_cf = requests.post(contacts_url,
                             headers={**bearer_hdr, "Content-Type": "application/json"},
                             json={},
                             timeout=15)
        if r_cf.status_code in (401, 403):
            r_cf = requests.post(contacts_url,
                                 params={"access_token": token},
                                 headers={"Content-Type": "application/json"},
                                 json={},
                                 timeout=15)
        print(f"  [{label}] contacts/filter: HTTP {r_cf.status_code}")
        if r_cf.status_code == 200:
            resp = r_cf.json() if r_cf.text else {}
            hits = resp.get("hits", []) if isinstance(resp, dict) else (resp if isinstance(resp, list) else [])
            if hits and isinstance(hits[0], dict):
                sample_keys = list(hits[0].keys())[:15]
                print(f"  [{label}] contacts hits sample keys: {sample_keys}")
                # Only proceed if hits have actual field data (not just $id)
                has_fields = any(k for k in sample_keys if k != "$id")
                if has_fields:
                    lead_f = next((k for k in ["Lead_Status","$Lead_Status","hs_lead_status"] if k in hits[0]), None)
                    agent_f = next((k for k in ["assigned_user","$assigned_user","last_edit_user"] if k in hits[0]), None)
                    if lead_f and agent_f:
                        for c in hits:
                            if not isinstance(c, dict): continue
                            sv = str(c.get(lead_f,"") or "").strip().upper()
                            ag = str(c.get(agent_f,"") or "").strip()
                            if not ag or ag in ("-","None",""): continue
                            bucket = None
                            if sv in {s.upper() for s in SELLER_STATUSES}: bucket = "seller"
                            elif sv in {s.upper() for s in RENTAL_STATUSES}: bucket = "rental"
                            elif sv in {s.upper() for s in EMAIL_STATUSES}: bucket = "email"
                            if bucket:
                                if ag not in result: result[ag] = {"seller":0,"rental":0,"email":0}
                                result[ag][bucket] += 1
                        if result:
                            print(f"  [{label}] contacts SUCCESS: {result}")
                            return result
                else:
                    print(f"  [{label}] contacts: hits only have $id, skipping to editsDef_v2")
    except Exception as e:
        print(f"  [{label}] contacts/filter error: {e}")

    # --- Approach 2: editsDef_v2 group0=Lead_Status, group1=user (primary, proven to work) ---
    try:
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
                    if isinstance(inner, list):
                        for sgrp in groups1:
                            if not isinstance(sgrp, dict): continue
                            status_val = str(sgrp.get("value", "")).strip().upper()
                            bucket = None
                            if status_val in {s.upper() for s in SELLER_STATUSES}: bucket = "seller"
                            elif status_val in {s.upper() for s in RENTAL_STATUSES}: bucket = "rental"
                            elif status_val in {s.upper() for s in EMAIL_STATUSES}: bucket = "email"
                            if bucket is None: continue
                            inner_grps = sgrp.get("groups", sgrp.get("children", []))
                            for u in (inner_grps if isinstance(inner_grps, list) else []):
                                if isinstance(u, dict):
                                    agent_name = str(u.get("value", ""))
                                    ucols = u.get("columns", [])
                                    count = 0
                                    if isinstance(ucols, list) and len(ucols) > 0:
                                        try: count = int(ucols[0]) if ucols[0] not in (None,"","-") else 0
                                        except (ValueError, TypeError): pass
                                    if agent_name and agent_name != "-":
                                        if agent_name not in result: result[agent_name]={"seller":0,"rental":0,"email":0}
                                        result[agent_name][bucket] += count
        if result:
            print(f"  [{label}] leads ap2 SUCCESS: {result}")
            return result
    except Exception as e:
        print(f"  [{label}] leads ap2 error: {e}")

    # --- Approach 3: editsDef_v2 group0=user, group1=Lead_Status ---
    try:
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
                for ugrp in groups2:
                    if not isinstance(ugrp, dict): continue
                    agent_name = str(ugrp.get("value", "")).strip()
                    if not agent_name or agent_name in ("-",""): continue
                    inner_grps = ugrp.get("groups", ugrp.get("children", []))
                    for sgrp in (inner_grps if isinstance(inner_grps, list) else []):
                        if not isinstance(sgrp, dict): continue
                        status_val = str(sgrp.get("value", "")).strip().upper()
                        scols = sgrp.get("columns", [])
                        count = 0
                        if isinstance(scols, list) and len(scols) > 0:
                            try: count = int(scols[0]) if scols[0] not in (None,"","-") else 0
                            except (ValueError, TypeError): pass
                        if agent_name and agent_name != "-":
                            bucket = None
                            if status_val in {s.upper() for s in SELLER_STATUSES}: bucket="seller"
                            elif status_val in {s.upper() for s in RENTAL_STATUSES}: bucket="rental"
                            elif status_val in {s.upper() for s in EMAIL_STATUSES}: bucket="email"
                            if bucket:
                                if agent_name not in result: result[agent_name]={"seller":0,"rental":0,"email":0}
                                result[agent_name][bucket] += count
        if result:
            print(f"  [{label}] leads ap3 SUCCESS: {result}")
            return result
    except Exception as e:
        print(f"  [{label}] leads ap3 error: {e}")

    print(f"  [{label}] fetch_lead_counts: all approaches failed, returning {{}}")
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
    # is_rm is determined by campaign in main(); default False here
    bench   = BENCHMARKS["rm_success_rate"]  # will be re-evaluated in main()
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



def fetch_campaign(cid, token, index, total, period_start, period_end, ts, campaign_label="", campaign_name=""):
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
                    for row in rows:
                        row["campaign_label"] = campaign_label
                        row["campaign_name"]  = campaign_name or campaign_label
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
        ch_name = fetch_campaign_name(ch_id, ch_tok)
        campaigns.append({"id": ch_id, "token": ch_tok, "label": "CLIENTHUB", "name": ch_name})
        print(f"  CLIENTHUB campaign: {ch_id} ({ch_name})")
    elif ch_id:
        print(f"  CLIENTHUB campaign: {ch_id} (NO TOKEN)")

    ch_new_id  = os.environ.get("CAMPAIGN_CLIENTHUB_NEW_ID", "").strip()
    ch_new_tok = os.environ.get("CAMPAIGN_CLIENTHUB_NEW_TOKEN", "").strip()
    if ch_new_id and ch_new_tok:
        ch_new_name = fetch_campaign_name(ch_new_id, ch_new_tok)
        campaigns.append({"id": ch_new_id, "token": ch_new_tok, "label": "CLIENTHUB", "name": ch_new_name})
        print(f"  CLIENTHUB_NEW campaign: {ch_new_id} ({ch_new_name})")
    elif ch_new_id:
        print(f"  CLIENTHUB_NEW campaign: {ch_new_id} (NO TOKEN)")

    i = 1
    while True:
        cid  = os.environ.get(f"CAMPAIGN_{i}_ID", "").strip()
        ctok = os.environ.get(f"CAMPAIGN_{i}_TOKEN", "").strip()
        if not cid:
            break
        if ctok:
            cname = fetch_campaign_name(cid, ctok)
            campaigns.append({"id": cid, "token": ctok, "label": f"CAMP{i}", "name": cname})
            print(f"  Campaign {i}: {cid} ({cname})")
        else:
            print(f"  Campaign {i}: {cid} (NO TOKEN)")
        i += 1

    leg_id  = os.environ.get("DIALFIRE_CAMPAIGN_ID", "").strip()
    leg_tok = os.environ.get("DIALFIRE_CAMPAIGN_TOKEN", "").strip()
    if leg_id and leg_tok:
        if not any(c["id"] == leg_id for c in campaigns):
            leg_name = fetch_campaign_name(leg_id, leg_tok)
            campaigns.append({"id": leg_id, "token": leg_tok, "label": "LEGACY", "name": leg_name})
            print(f"  Legacy campaign: {leg_id} ({leg_name})")

    if not campaigns:
        print("No campaigns configured.")
        return

    print(f"Total campaigns: {len(campaigns)}")
    print()

    all_rows = []
    for idx, c in enumerate(campaigns):
        rows = fetch_campaign(c["id"], c["token"], idx, len(campaigns),
                              period_start, period_end, ts,
                              campaign_label=c.get("label", ""),
                              campaign_name=c.get("name", ""))
        all_rows.extend(rows)

    print()
    print(f"Raw rows collected: {len(all_rows)}")

    merged = {}
    agent_campaigns = {}  # name -> list of campaign names this agent appeared in
    for row in all_rows:
        agent = parse_row(row)
        if agent is None:
            continue
        name = agent["name"]
        cname = row.get("campaign_name", row.get("campaign_label", ""))
        # Track which campaigns this agent appeared in
        if name not in agent_campaigns:
            agent_campaigns[name] = []
        if cname and cname not in agent_campaigns[name]:
            agent_campaigns[name].append(cname)
        # Mark as RM if they appear in the CLIENTHUB campaign
        if row.get("campaign_label", "") == "CLIENTHUB":
            agent["is_rm"] = True
        if name in merged:
            ex = merged[name]
            ex["calls"]    += agent["calls"]
            ex["success"]  += agent["success"]
            ex["seller"]   += agent["seller"]
            ex["rental"]   += agent["rental"]
            ex["email"]    += agent["email"]
            ex["workTime"]  = round(ex["workTime"] + agent["workTime"], 2)
            # Once flagged as RM (CLIENTHUB), keep that flag
            if agent.get("is_rm"):
                ex["is_rm"] = True
        else:
            merged[name] = agent

    # Attach campaign list to each agent
    for name, agent in merged.items():
        agent["campaigns"] = agent_campaigns.get(name, [])

    agents = list(merged.values())
    for a in agents:
        a["cph"] = round(a["calls"] / a["workTime"], 1) if a["workTime"] > 0 else 0.0
        is_rm    = a.get("is_rm", False)
        bench    = BENCHMARKS["rm_success_rate"] if is_rm else BENCHMARKS["fc_success_rate"]
        a["meetsTarget"] = a["cph"] >= BENCHMARKS["cph"] and a["successRate"] >= bench

    rm_agents    = sorted([a for a in agents if a.get("is_rm", False)],      key=lambda x: -x["calls"])
    fancy_agents = sorted([a for a in agents if not a.get("is_rm", False)],   key=lambda x: -x["calls"])

    print(f"Unique agents: {len(agents)}")
    print(f"RM: {len(rm_agents)} | Fancy: {len(fancy_agents)}")
    for a in rm_agents:
        print(f"  RM   {a['name']:<22} calls={a['calls']:>4} success={a['success']:>3} "
              f"seller={a['seller']:>3} rental={a['rental']:>3} email={a['email']:>3} cph={a['cph']:>5} campaigns={a.get('campaigns')}")
    for a in fancy_agents:
        print(f"  FANCY {a['name']:<22} calls={a['calls']:>4} success={a['success']:>3} "
              f"seller={a['seller']:>3} rental={a['rental']:>3} email={a['email']:>3} cph={a['cph']:>5} campaigns={a.get('campaigns')}")

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
