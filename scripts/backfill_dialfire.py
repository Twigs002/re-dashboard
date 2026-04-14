"""
DialFire Historical Backfill Script
=====================================
Fetches every Mon–Sun week between START_DATE and today,
and writes each week into history.json.

Run this manually via GitHub Actions (backfill.yml workflow)
to import historical data for a date range.

Environment variables:
  DIALFIRE_CAMPAIGNS   (same secret as daily script — required)
  START_DATE           e.g. "2026-03-01" — required
  END_DATE             e.g. "2026-04-13" — optional, defaults to last completed Sunday
"""

import os, json, time, requests
from datetime import datetime, timedelta, timezone, date

# ── Load campaigns ────────────────────────────────────────────────
raw = os.environ.get("DIALFIRE_CAMPAIGNS", "[]")
try:
    CAMPAIGNS = json.loads(raw)
except json.JSONDecodeError as e:
    print(f"❌ Could not parse DIALFIRE_CAMPAIGNS: {e}")
    raise

if not CAMPAIGNS:
    raise ValueError("DIALFIRE_CAMPAIGNS is empty.")

print(f"✓ {len(CAMPAIGNS)} campaigns loaded")

# ── RM classification ─────────────────────────────────────────────
RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

def is_rm(name):
    n = name.lower()
    return any(rm.lower() in n or n in rm.lower() for rm in RM_NAMES)

# ── Build list of all Mon–Sun weeks in range ──────────────────────
def get_weeks(start_str, end_str):
    """
    Returns a list of (monday, sunday) date pairs covering the range.
    Partial weeks at the end are included as-is.
    """
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end   = datetime.strptime(end_str,   "%Y-%m-%d").date()

    # Snap start back to the Monday of that week
    monday = start - timedelta(days=start.weekday())

    weeks = []
    while monday <= end:
        sunday = monday + timedelta(days=6)
        if sunday > end:
            sunday = end   # partial final week — use today as end
        weeks.append((monday, sunday))
        monday += timedelta(days=7)
    return weeks

# ── Determine date range ──────────────────────────────────────────
start_date = os.environ.get("START_DATE", "")
if not start_date:
    raise ValueError("START_DATE environment variable is required (e.g. 2026-03-01)")

today_date  = datetime.now(timezone.utc).date()
# Default end: yesterday (most recently completed day)
end_date    = os.environ.get("END_DATE", str(today_date - timedelta(days=1)))

weeks = get_weeks(start_date, end_date)
print(f"\n📅 Backfill range: {start_date} → {end_date}")
print(f"📦 Weeks to fetch: {len(weeks)}\n")

# ── Fetch one campaign for one week ──────────────────────────────
def fetch_campaign(campaign, date_from, date_to):
    cid   = campaign["id"]
    token = campaign["token"]
    label = campaign.get("name", cid)

    base    = f"https://app.dialfire.com/api/campaigns/{cid}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params  = {
        "from":       str(date_from),
        "to":         str(date_to),
        "groupBy":    "agent",
        "reportType": "processing",
    }

    try:
        r = requests.get(f"{base}/firebase/reports/calls",
                         headers=headers, params=params, timeout=20)
        if r.status_code == 401:
            print(f"    ⚠ [{label}] Unauthorized — skipping")
            return []
        if r.status_code == 404:
            r = requests.get(f"{base}/reports/contacts",
                             headers=headers, params=params, timeout=20)
        r.raise_for_status()
        raw = r.json()
        rows = raw if isinstance(raw, list) else raw.get("data", raw.get("rows", []))
        return rows
    except requests.RequestException as e:
        print(f"    ✗ [{label}] {e}")
        return []

def parse_row(row, campaign_name):
    name = (row.get("agent_name") or row.get("username")
            or row.get("user")    or row.get("name", "Unknown")).strip()
    calls   = int(row.get("total_calls")   or row.get("calls",   0) or 0)
    success = int(row.get("total_success") or row.get("success", 0) or 0)
    rental  = int(row.get("rental_lead")   or row.get("rental",  0) or 0)
    seller  = int(row.get("seller_lead")   or row.get("seller",  0) or 0)
    email   = int(row.get("got_email")     or row.get("email",   0) or 0)
    wt_raw  = float(row.get("work_time") or row.get("worktime") or row.get("dial_time") or 0)
    work_time = round(wt_raw / 3600, 2) if wt_raw > 1000 else round(wt_raw, 2)
    return {
        "name": name, "calls": calls, "success": success,
        "rental": rental, "seller": seller, "email": email,
        "workTime": work_time, "_campaigns": [campaign_name],
    }

def merge_agents(all_rows):
    merged = {}
    for row in all_rows:
        name = row["name"]
        if not name or name.lower() in ("unknown", "system", ""):
            continue
        if name in merged:
            m = merged[name]
            m["calls"]    += row["calls"]
            m["success"]  += row["success"]
            m["rental"]   += row["rental"]
            m["seller"]   += row["seller"]
            m["email"]    += row["email"]
            m["workTime"]  = round(m["workTime"] + row["workTime"], 2)
            m["_campaigns"] = list(set(m["_campaigns"] + row["_campaigns"]))
        else:
            merged[name] = dict(row)
    return list(merged.values())

def div_string(campaigns_list):
    return " / ".join(sorted(set(c for c in campaigns_list if c)))

# ── Main backfill loop ────────────────────────────────────────────
def main():
    data_dir  = os.path.join(os.path.dirname(__file__), "..", "data")
    hist_path = os.path.join(data_dir, "history.json")

    # Load existing history so we don't overwrite it
    history = {}
    if os.path.exists(hist_path):
        with open(hist_path) as f:
            history = json.load(f)
    print(f"📂 Existing history entries: {len(history)}\n")

    total_weeks = len(weeks)
    for wi, (monday, sunday) in enumerate(weeks, 1):
        date_from = str(monday)
        date_to   = str(sunday)
        key       = date_from  # week keyed by Monday

        print(f"\n[{wi}/{total_weeks}] Week {date_from} → {date_to}")

        if key in history:
            print(f"  ⏭  Already in history — skipping (delete the key to re-fetch)")
            continue

        all_rows = []
        for campaign in CAMPAIGNS:
            rows = fetch_campaign(campaign, monday, sunday)
            for row in rows:
                parsed = parse_row(row, campaign.get("name", campaign["id"]))
                if parsed["calls"] > 0:
                    all_rows.append(parsed)
            time.sleep(0.25)  # polite delay per campaign

        agents = merge_agents(all_rows)
        rm, fancy = [], []
        for a in agents:
            div   = div_string(a["_campaigns"])
            clean = {k: v for k, v in a.items() if k != "_campaigns"}
            if is_rm(a["name"]):
                rm.append(clean)
            else:
                fancy.append({**clean, "div": div})

        total_calls = sum(a["calls"] for a in agents)
        print(f"  ✓ {len(agents)} agents · {total_calls:,} calls · {len(rm)} RM · {len(fancy)} Fancy")

        history[key] = {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rm":        sorted(rm,    key=lambda x: x["calls"], reverse=True),
            "fancy":     sorted(fancy, key=lambda x: x["calls"], reverse=True),
        }

    # Save the full updated history
    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)

    print(f"\n{'='*50}")
    print(f"✅ Backfill complete — {len(history)} weeks now in history.json")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
