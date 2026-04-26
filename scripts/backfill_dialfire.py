"""
DialFire Historical Backfill Script
=====================================
Fetches every Mon-Sun week between START_DATE and END_DATE,
and writes each week into history.json.

Uses the same api.dialfire.com + access_token + editsDef_v2 approach as fetch_dialfire.py.
Converts absolute dates to relative timespans (e.g. "36-30day") for the editsDef_v2 endpoint.
Skips weeks that already have real agent data (rm or fancy not empty).

Environment variables:
  CAMPAIGN_CLIENTHUB_ID / CAMPAIGN_CLIENTHUB_TOKEN  (preferred)
  CAMPAIGN_1_ID / CAMPAIGN_1_TOKEN                  (optional extra campaigns)
  CAMPAIGN_2_ID / CAMPAIGN_2_TOKEN                  (optional extra campaigns)
  DIALFIRE_CAMPAIGNS  (fallback JSON list)
  START_DATE          e.g. "2026-03-01" -- required
  END_DATE            e.g. "2026-04-13" -- optional, defaults to yesterday
"""

import os, json, time, requests
from datetime import datetime, timedelta, timezone, date as date_type

LOCALE = "en_US"
API_BASE = "https://api.dialfire.com"

RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

BENCHMARKS = {"cph": 45, "daily_calls": 315, "rm_success_rate": 17, "fc_success_rate": 20}

SELLER_STATUSES = {"LEAD"}
RENTAL_STATUSES = {"RENTAL_LEAD"}
EMAIL_STATUSES  = {"GOT_EMAIL"}

# Load campaigns (individual vars first, then DIALFIRE_CAMPAIGNS JSON)
CAMPAIGNS = []
ch_id  = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
ch_tok = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()
if ch_id and ch_tok:
    CAMPAIGNS.append({"id": ch_id, "token": ch_tok, "name": "CLIENTHUB"})

i = 1
while True:
    cid = os.environ.get(f"CAMPAIGN_{i}_ID", "").strip()
    tok = os.environ.get(f"CAMPAIGN_{i}_TOKEN", "").strip()
    if not cid or not tok:
        break
    CAMPAIGNS.append({"id": cid, "token": tok, "name": f"CAMP{i}"})
    i += 1

if not CAMPAIGNS:
    raw = os.environ.get("DIALFIRE_CAMPAIGNS", "")
    if raw:
        try:
            for c in json.loads(raw):
                if c.get("id") and c.get("token"):
                    CAMPAIGNS.append(c)
        except Exception as e:
            print(f"Warning: could not parse DIALFIRE_CAMPAIGNS: {e}")

if not CAMPAIGNS:
    raise SystemExit("ERROR: No campaigns configured.")

print(f"Campaigns loaded: {[c['name'] for c in CAMPAIGNS]}")


def get_weeks(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end   = datetime.strptime(end_str,   "%Y-%m-%d").date()
    monday = start - timedelta(days=start.weekday())
    weeks = []
    while monday <= end:
        sunday = monday + timedelta(days=6)
        if sunday > end:
            sunday = end
        weeks.append((monday, sunday))
        monday += timedelta(days=7)
    return weeks


def dates_to_timespan(date_from, date_to):
    """Convert absolute dates to Dialfire relative timespan format.
    Dialfire timespan 'X-Yday' means from X days ago to Y days ago (from today UTC).
    We add 1 to the end to include the full end day.
    """
    today = datetime.now(timezone.utc).date()
    days_from = (today - date_from).days
    days_to   = (today - date_to).days - 1  # -1 to include end day
    if days_to < 0:
        days_to = 0
    return f"{days_from}-{days_to}day"


def fetch_json(url, params, label, tag, max_poll=10):
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 202:
            loc = r.headers.get("Location") or r.headers.get("location")
            for _ in range(max_poll):
                time.sleep(3)
                r2 = requests.get(loc, timeout=30) if loc else r
                if r2.status_code == 200:
                    try:
                        return r2.json()
                    except Exception:
                        return {}
                if r2.status_code in (401, 403):
                    print(f"  [{label}] {tag} -> {r2.status_code}")
                    return None
            return {}
        if r.status_code in (401, 403):
            print(f"  [{label}] {tag} -> HTTP {r.status_code} (token issue)")
            return None
        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                print(f"  [{label}] JSON error: {e} | body={r.text[:200]}")
                return {}
        print(f"  [{label}] {tag} -> HTTP {r.status_code} | {r.text[:100]}")
        return {}
    except Exception as e:
        print(f"  [{label}] {tag} -> error: {e}")
        return {}


def fetch_lead_counts_bf(cid, token, ts, label):
    """Fetch lead counts per agent using editsDef_v2 group0=Lead_Status group1=user."""
    result = {}
    base_url = f"{API_BASE}/api/campaigns/{cid}/reports/editsDef_v2/report/{LOCALE}"
    try:
        params = {
            "access_token": token,
            "asTree": "true",
            "timespan": ts,
            "group0": "Lead_Status",
            "group1": "user",
            "column0": "completed",
        }
        data = fetch_json(base_url, params, label, "leads: Lead_Status>user")
        if data and isinstance(data, dict):
            for sgrp in data.get("groups", []):
                if not isinstance(sgrp, dict): continue
                sv = str(sgrp.get("value", "")).strip().upper()
                bucket = None
                if sv in {s.upper() for s in SELLER_STATUSES}: bucket = "seller"
                elif sv in {s.upper() for s in RENTAL_STATUSES}: bucket = "rental"
                elif sv in {s.upper() for s in EMAIL_STATUSES}: bucket = "email"
                if bucket is None: continue
                for u in sgrp.get("groups", sgrp.get("children", [])):
                    if not isinstance(u, dict): continue
                    ag = str(u.get("value", ""))
                    ucols = u.get("columns", [])
                    cnt = 0
                    if ucols:
                        try: cnt = int(ucols[0]) if ucols[0] not in (None,"","-") else 0
                        except: pass
                    if ag and ag != "-":
                        if ag not in result: result[ag] = {"seller":0,"rental":0,"email":0}
                        result[ag][bucket] += cnt
    except Exception as e:
        print(f"  [{label}] fetch_lead_counts_bf error: {e}")
    return result

def fetch_campaign_week(campaign, date_from, date_to):
    cid   = campaign["id"]
    token = campaign["token"]
    label = campaign.get("name", cid)
    base  = f"{API_BASE}/api/campaigns/{cid}"

    ts = dates_to_timespan(date_from, date_to)
    print(f"  [{label}] timespan={ts} (for {date_from} -> {date_to})")

    # Use editsDef_v2 with relative timespan -- same as daily fetch_dialfire.py
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

    data = fetch_json(f"{base}/reports/editsDef_v2/report/{LOCALE}", params, label, f"editsDef_v2 ts={ts}")
    if data is None:
        print(f"  [{label}] 403 -- token invalid, skipping campaign")
        return []
    if not data:
        print(f"  [{label}] No data returned")
        return []

    grp = data.get("groups", [])
    if isinstance(grp, list) and len(grp) > 0:
        print(f"  [{label}] editsDef_v2 -> {len(grp)} groups")
        # Fetch lead counts and attach to each group row
        lead_counts = fetch_lead_counts_bf(cid, token, ts, label)
        if lead_counts:
            print(f"  [{label}] lead counts: {lead_counts}")
            for item in grp:
                if isinstance(item, dict):
                    ag_name = str(item.get("value","")).strip()
                    if ag_name in lead_counts:
                        item["seller"] = lead_counts[ag_name]["seller"]
                        item["rental"] = lead_counts[ag_name]["rental"]
                        item["email"]  = lead_counts[ag_name]["email"]
        return grp

    print(f"  [{label}] editsDef_v2 -> empty groups")
    return []


def parse_row(row):
    name = str(row.get("value") or row.get("name") or row.get("user") or row.get("username") or row.get("agent_name") or "Unknown").strip()

    calls   = int(row.get("completed")  or row.get("calls",   0) or 0)
    success = int(row.get("success")    or 0)
    wt_raw  = float(row.get("workTime") or 0)
    # workTime from editsDef_v2 is in hours; >1000 means it was in ms
    work_hrs = wt_raw / 3600000 if wt_raw > 1000 else wt_raw

    cph = round(calls / work_hrs, 1) if work_hrs > 0 else 0.0
    sr  = round(success / calls * 100, 1) if calls > 0 else 0.0

    is_rm     = name in RM_NAMES
    bench_sr  = BENCHMARKS["rm_success_rate"] if is_rm else BENCHMARKS["fc_success_rate"]
    meets_tgt = (cph >= BENCHMARKS["cph"] and sr >= bench_sr) if calls > 0 else False

    return {
        "name":        name,
        "calls":       calls,
        "success":     success,
        "seller":      int(row.get("seller_lead") or row.get("seller") or 0),
        "rental":      int(row.get("rental_lead") or row.get("rental") or 0),
        "email":       int(row.get("got_email")   or row.get("email")  or 0),
        "cph":         cph,
        "successRate": sr,
        "workTime":    round(work_hrs, 4),
        "meetsTarget": meets_tgt,
    }


def main():
    start_date = (os.environ.get("START_DATE") or "").strip()
    if not start_date:
        raise ValueError("START_DATE is required (e.g. 2026-03-01)")

    today_date = datetime.now(timezone.utc).date()
    end_date   = (os.environ.get("END_DATE") or "").strip() or str(today_date - timedelta(days=1))

    weeks = get_weeks(start_date, end_date)
    print(f"\n Backfill range: {start_date} to {end_date}")
    print(f" Weeks to fetch: {len(weeks)}\n")

    hist_path = "data/history.json"
    try:
        with open(hist_path) as f:
            history = json.load(f)
        if isinstance(history, dict):
            history = list(history.values())
        if not isinstance(history, list):
            history = []
    except (FileNotFoundError, json.JSONDecodeError):
        history = []

    # Build a set of week keys that have REAL data (non-empty rm or fancy)
    weeks_with_data = set()
    for e in history:
        has_data = (len(e.get("rm", [])) > 0) or (len(e.get("fancy", [])) > 0)
        if has_data:
            if e.get("weekStart"):
                weeks_with_data.add(e["weekStart"])
            if e.get("week"):
                weeks_with_data.add(e["week"])

    print(f" Existing history entries: {len(history)}")
    print(f" Weeks with real data: {len(weeks_with_data)}")

    total_weeks = len(weeks)
    for week_idx, (date_from, date_to) in enumerate(weeks):
        key = str(date_from)
        print(f"\n***{week_idx+1}/{total_weeks}*** Week {date_from} -> {date_to}")

        if key in weeks_with_data:
            print(f"  Already has data -- skipping")
            continue

        agents = {}
        for campaign in CAMPAIGNS:
            rows = fetch_campaign_week(campaign, date_from, date_to)
            for row in rows:
                parsed = parse_row(row)
                n = parsed["name"]
                if not n or n == "Unknown":
                    continue
                if n not in agents:
                    agents[n] = parsed.copy()
                else:
                    a = agents[n]
                    a["calls"]   += parsed["calls"]
                    a["success"] += parsed["success"]
                    a["seller"]  += parsed["seller"]
                    a["rental"]  += parsed["rental"]
                    a["email"]   += parsed["email"]
                    total_wt = a["workTime"] + parsed["workTime"]
                    a["workTime"] = round(total_wt, 4)
                    a["cph"] = round(a["calls"] / total_wt, 1) if total_wt > 0 else 0.0
                    a["successRate"] = round(a["success"] / a["calls"] * 100, 1) if a["calls"] > 0 else 0.0
                    is_rm = n in RM_NAMES
                    bench_sr = BENCHMARKS["rm_success_rate"] if is_rm else BENCHMARKS["fc_success_rate"]
                    a["meetsTarget"] = (a["cph"] >= BENCHMARKS["cph"] and a["successRate"] >= bench_sr) if a["calls"] > 0 else False

        rm    = [v for v in agents.values() if v["name"] in RM_NAMES]
        fancy = [v for v in agents.values() if v["name"] not in RM_NAMES]

        print(f"  {len(agents)} agents, {sum(v['calls'] for v in agents.values())} calls, {len(rm)} RM, {len(fancy)} Fancy")

        history = [e for e in history if e.get("weekStart") != key and e.get("week") != key]
        history.insert(0, {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "week":      key,
            "weekStart": key,
            "weekEnd":   str(date_to),
            "rm":        sorted(rm,    key=lambda x: x["calls"], reverse=True),
            "fancy":     sorted(fancy, key=lambda x: x["calls"], reverse=True),
        })

    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)

    print(f"\n{'='*50}")
    print(f"Backfill complete -- {len(history)} weeks in history.json")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
