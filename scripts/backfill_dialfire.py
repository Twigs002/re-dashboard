"""
DialFire Historical Backfill Script
=====================================
Fetches every Mon-Sun week between START_DATE and END_DATE,
and writes each week into history.json.

Uses the same api.dialfire.com + access_token approach as fetch_dialfire.py.

Environment variables:
  CAMPAIGN_CLIENTHUB_ID / CAMPAIGN_CLIENTHUB_TOKEN  (preferred)
  CAMPAIGN_1_ID / CAMPAIGN_1_TOKEN                  (optional extra campaigns)
  CAMPAIGN_2_ID / CAMPAIGN_2_TOKEN                  (optional extra campaigns)
  DIALFIRE_CAMPAIGNS  (fallback JSON list)
  START_DATE          e.g. "2026-03-01" -- required
  END_DATE            e.g. "2026-04-13" -- optional, defaults to yesterday
"""

import os, json, time, requests
from datetime import datetime, timedelta, timezone, date

LOCALE = "en_US"
API_BASE = "https://api.dialfire.com"

RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

BENCHMARKS = {"cph": 45, "daily_calls": 315, "rm_success_rate": 17, "fc_success_rate": 20}

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


def fetch_json(url, params, label, tag, max_poll=8):
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
                if r2.status_code == 403:
                    print(f"  [{label}] {tag} -> 403")
                    return None
            return {}
        if r.status_code in (401, 403):
            print(f"  [{label}] {tag} -> HTTP {r.status_code} (token issue)")
            return None
        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                print(f"  [{label}] JSON error: {e}")
                return {}
        print(f"  [{label}] {tag} -> HTTP {r.status_code}")
        return {}
    except Exception as e:
        print(f"  [{label}] {tag} -> error: {e}")
        return {}


def fetch_campaign_week(campaign, date_from, date_to):
    cid   = campaign["id"]
    token = campaign["token"]
    label = campaign.get("name", cid)
    base  = f"{API_BASE}/api/campaigns/{cid}"

    attempts = [
        {
            "url": f"{base}/firebase/reports/calls",
            "params": {
                "access_token": token,
                "from": str(date_from),
                "to":   str(date_to),
                "groupBy": "agent",
                "reportType": "processing",
            },
            "tag": "firebase/reports/calls from/to",
        },
        {
            "url": f"{base}/reports/editsDef_v2/report/{LOCALE}",
            "params": {
                "access_token": token,
                "asTree": "true",
                "from": str(date_from),
                "to":   str(date_to),
                "group0": "user",
                "column0": "completed",
                "column1": "success",
                "column2": "successRate",
                "column3": "workTime",
            },
            "tag": "editsDef_v2 from/to",
        },
    ]

    for attempt in attempts:
        data = fetch_json(attempt["url"], attempt["params"], label, attempt["tag"])
        if data is None:
            print(f"  [{label}] 403 -- token invalid, skipping campaign")
            return []
        if not data:
            continue

        rows = []
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            grp = data.get("groups", [])
            if isinstance(grp, list) and len(grp) > 0:
                rows = grp
            else:
                d = data.get("data", data.get("rows", []))
                if isinstance(d, list):
                    rows = d

        if rows:
            print(f"  [{label}] {attempt['tag']} -> {len(rows)} rows")
            return rows

    print(f"  [{label}] No data for {date_from} -> {date_to}")
    return []


def parse_row(row):
    name = (
        row.get("agent_name") or row.get("username") or
        row.get("user") or row.get("value") or row.get("name", "Unknown")
    ).strip()

    calls   = int(row.get("completed")  or row.get("total_calls")   or row.get("calls",   0) or 0)
    success = int(row.get("success")    or row.get("total_success") or 0)
    wt_raw  = float(row.get("workTime") or row.get("work_time")     or 0)
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

    existing_keys = set()
    for e in history:
        if e.get("weekStart"):
            existing_keys.add(e["weekStart"])
        if e.get("week"):
            existing_keys.add(e["week"])

    print(f" Existing history entries: {len(history)}")

    total_weeks = len(weeks)
    for week_idx, (date_from, date_to) in enumerate(weeks):
        key = str(date_from)
        print(f"\n***{week_idx+1}/{total_weeks}*** Week {date_from} -> {date_to}")

        if key in existing_keys:
            print(f"  Already in history -- skipping")
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
