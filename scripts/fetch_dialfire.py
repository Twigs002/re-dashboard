"""
DialFire Multi-Campaign → weekly_data.json fetcher
=====================================================
Loops through ALL 90+ campaigns, pulls agent stats from each,
then merges agents who appear in multiple campaigns.

Required GitHub Secrets (Settings → Secrets and variables → Actions):
──────────────────────────────────────────────────────────────────────
  DIALFIRE_CAMPAIGNS   A JSON array of all your campaign configs, e.g.:
  [
    {"id": "AC9EUK7GW85HJW3U", "token": "nkwWPjff...", "name": "Llamas"},
    {"id": "XXXXXXXXXXXX",     "token": "abc123...",   "name": "Proteas"},
    ...
  ]

  The "name" field is optional but helps with debugging.

HOW TO BUILD YOUR CAMPAIGNS JSON:
──────────────────────────────────
1. In LookerStudio, open each DialFire data source
2. Copy the Campaign ID and Campaign Token for each one
3. Build the JSON array (you can use the template in campaigns_template.json)
4. Paste the entire JSON into a single GitHub Secret called DIALFIRE_CAMPAIGNS
"""

import os, json, time, requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ── Load all campaign configs from one GitHub Secret ──────────────
raw_campaigns = os.environ.get("DIALFIRE_CAMPAIGNS", "[]")
try:
    CAMPAIGNS = json.loads(raw_campaigns)
except json.JSONDecodeError as e:
    print(f"❌ Could not parse DIALFIRE_CAMPAIGNS secret: {e}")
    print("   Make sure it is valid JSON. See README for format.")
    raise

if not CAMPAIGNS:
    raise ValueError("DIALFIRE_CAMPAIGNS secret is empty. Add your campaign list.")

print(f"✓ Loaded {len(CAMPAIGNS)} campaigns from secret")

# ── Date range: last full Mon–Sun week ────────────────────────────
today    = datetime.now(timezone.utc).date()
last_mon = today - timedelta(days=today.weekday() + 7)
last_sun = last_mon + timedelta(days=6)
DATE_FROM = last_mon.strftime("%Y-%m-%d")
DATE_TO   = last_sun.strftime("%Y-%m-%d")

# ── Classify RM vs Fancy Caller ───────────────────────────────────
# Update this list when agents move between groups
RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

def is_rm(name):
    n = name.lower()
    return any(rm.lower() in n or n in rm.lower() for rm in RM_NAMES)

# ── Fetch one campaign ────────────────────────────────────────────
def fetch_campaign(campaign):
    cid   = campaign["id"]
    token = campaign["token"]
    label = campaign.get("name", cid)

    base    = f"https://app.dialfire.com/api/campaigns/{cid}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
    }
    params  = {
        "from":       DATE_FROM,
        "to":         DATE_TO,
        "groupBy":    "agent",
        "reportType": "processing",
    }

    try:
        r = requests.get(f"{base}/firebase/reports/calls",
                         headers=headers, params=params, timeout=20)
        if r.status_code == 401:
            print(f"  ⚠ [{label}] Unauthorized — check token")
            return []
        if r.status_code == 404:
            # Try alternate endpoint
            r = requests.get(f"{base}/reports/contacts",
                             headers=headers, params=params, timeout=20)
        r.raise_for_status()
        raw = r.json()
        rows = raw if isinstance(raw, list) else raw.get("data", raw.get("rows", []))
        print(f"  ✓ [{label}] {len(rows)} agent rows")
        return rows

    except requests.RequestException as e:
        print(f"  ✗ [{label}] Failed: {e}")
        return []

# ── Parse one row into our standard schema ────────────────────────
def parse_row(row, campaign_name):
    name = (row.get("agent_name") or row.get("username")
            or row.get("user")    or row.get("name", "Unknown")).strip()

    calls   = int(row.get("total_calls")   or row.get("calls",   0) or 0)
    success = int(row.get("total_success") or row.get("success", 0) or 0)
    rental  = int(row.get("rental_lead")   or row.get("rental",  0) or 0)
    seller  = int(row.get("seller_lead")   or row.get("seller",  0) or 0)
    email   = int(row.get("got_email")     or row.get("email",   0) or 0)

    wt_raw = float(row.get("work_time") or row.get("worktime")
                   or row.get("dial_time") or 0)
    # DialFire returns seconds if > 1000, otherwise decimal hours
    work_time = round(wt_raw / 3600, 2) if wt_raw > 1000 else round(wt_raw, 2)

    return {
        "name":      name,
        "calls":     calls,
        "success":   success,
        "rental":    rental,
        "seller":    seller,
        "email":     email,
        "workTime":  work_time,
        "_campaigns": [campaign_name],   # track which campaigns this agent appeared in
    }

# ── Merge agents who appear across multiple campaigns ─────────────
def merge_agents(all_rows):
    """
    Agents like TamzinJacobs appear in multiple campaigns (divisions).
    We sum their numbers and track which campaigns/divisions they appear in.
    """
    merged = {}
    for row in all_rows:
        name = row["name"]
        if not name or name.lower() in ("unknown", "system", ""):
            continue
        if name in merged:
            m = merged[name]
            m["calls"]     += row["calls"]
            m["success"]   += row["success"]
            m["rental"]    += row["rental"]
            m["seller"]    += row["seller"]
            m["email"]     += row["email"]
            m["workTime"]  = round(m["workTime"] + row["workTime"], 2)
            m["_campaigns"] = list(set(m["_campaigns"] + row["_campaigns"]))
        else:
            merged[name] = dict(row)
    return list(merged.values())

# ── Format division string from campaign list ──────────────────────
def div_string(campaigns_list):
    return " / ".join(sorted(set(c for c in campaigns_list if c)))

# ── Main ──────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*55}")
    print(f"DialFire Multi-Campaign Fetcher")
    print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Week: {DATE_FROM} → {DATE_TO}")
    print(f"Campaigns: {len(CAMPAIGNS)}")
    print(f"{'='*55}\n")

    all_rows = []
    failed   = []

    for i, campaign in enumerate(CAMPAIGNS, 1):
        print(f"[{i}/{len(CAMPAIGNS)}] {campaign.get('name', campaign['id'])}")
        rows = fetch_campaign(campaign)
        for row in rows:
            parsed = parse_row(row, campaign.get("name", campaign["id"]))
            if parsed["calls"] > 0:  # skip zero-activity rows
                all_rows.append(parsed)
        # Small delay to be polite to the API
        time.sleep(0.3)

    print(f"\n{'─'*40}")
    print(f"Raw rows fetched: {len(all_rows)}")

    agents  = merge_agents(all_rows)
    print(f"Unique agents after merge: {len(agents)}")

    rm, fancy = [], []
    for a in agents:
        div = div_string(a["_campaigns"])
        # Remove internal tracking key
        clean = {k: v for k, v in a.items() if k != "_campaigns"}
        if is_rm(a["name"]):
            rm.append(clean)
        else:
            fancy.append({**clean, "div": div})

    print(f"RM: {len(rm)}  |  Fancy Callers: {len(fancy)}")

    output = {
        "week":      DATE_FROM,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rm":        sorted(rm,    key=lambda x: x["calls"], reverse=True),
        "fancy":     sorted(fancy, key=lambda x: x["calls"], reverse=True),
    }

    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")

    # Save current week snapshot
    with open(os.path.join(data_dir, "weekly_data.json"), "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n✅ Saved → data/weekly_data.json")

    # Append to history.json (used by date range reports + caller comparison)
    hist_path = os.path.join(data_dir, "history.json")
    history = {}
    if os.path.exists(hist_path):
        with open(hist_path) as f:
            history = json.load(f)
    history[DATE_FROM] = {
        "generated": output["generated"],
        "rm":        output["rm"],
        "fancy":     output["fancy"],
    }
    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"✅ Appended → data/history.json ({len(history)} weeks stored)")

    if failed:
        print(f"⚠  {len(failed)} campaigns failed — check tokens above")

if __name__ == "__main__":
    main()
