# 🏠 Real Estate Weekly Performance Dashboard

A live, auto-updating performance dashboard for RM and Fancy Caller teams.
Hosted on **GitHub Pages** — refreshed every weekday morning via **GitHub Actions** using the DialFire API.

---

## 🚀 One-time Setup (10 minutes)

### Step 1 — Create a GitHub Repository

1. Go to [github.com](https://github.com) and click **New repository**
2. Name it `re-dashboard` (or whatever you like)
3. Set it to **Public** (required for free GitHub Pages)
4. Click **Create repository**

### Step 2 — Upload these files

**Option A — via GitHub website (easiest):**
1. Open the repo you just created
2. Click **Add file → Upload files**
3. Drag and drop the entire `re-dashboard` folder contents
4. Click **Commit changes**

**Option B — via Git (if you're comfortable with terminal):**
```bash
cd re-dashboard
git init
git remote add origin https://github.com/YOUR_USERNAME/re-dashboard.git
git add .
git commit -m "Initial dashboard setup"
git push -u origin main
```

### Step 3 — Add your DialFire API credentials as GitHub Secrets

> ⚠️ **Never put your API token directly in any file** — always use Secrets.

1. In your repo, go to **Settings → Secrets and variables → Actions**
2. Click **New repository secret** and add:

| Secret Name | Value |
|---|---|
| `DIALFIRE_CAMPAIGN_ID` | `AC9EUK7GW85HJW3U` |
| `DIALFIRE_CAMPAIGN_TOKEN` | *(your full campaign token)* |

### Step 4 — Enable GitHub Pages

1. Go to **Settings → Pages**
2. Under **Source**, select **Deploy from a branch**
3. Choose branch: `main`, folder: `/ (root)`
4. Click **Save**
5. After ~60 seconds, your dashboard will be live at:
   ```
   https://YOUR_USERNAME.github.io/re-dashboard/
   ```

### Step 5 — Test the automation manually

1. Go to **Actions** tab in your repo
2. Click **🔄 Daily DialFire Data Refresh**
3. Click **Run workflow → Run workflow**
4. Watch it run — if green ✅, your data is live!

---

## 📅 Automation Schedule

The dashboard auto-refreshes **Monday–Friday at 7:00 AM SAST**.

You can also trigger it manually anytime from the **Actions** tab.

---

## ⏱ ConnectTeams Work Time Comparison

The dashboard includes a **Work Time Comparison** tab:

1. Open ConnectTeams → Reports → Time Tracking
2. Select the matching week
3. Export as **CSV**
4. Open the dashboard → click **Work Time Comparison** tab
5. Upload the CSV — comparison appears instantly

Expected CSV columns: `Name, Date, Clock In, Clock Out, Total Hours`
(The dashboard is flexible and will try to auto-detect column names)

---

## 🔧 Updating Agent Groups

If agents move between RM and Fancy Caller groups, update `scripts/fetch_dialfire.py`:

```python
RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
    # Add or remove names here
}
```

And update `DIVISION_MAP` to assign divisions to Fancy Callers.

---

## 📊 Benchmarks

| Metric | RM Target | Fancy Caller Target |
|---|---|---|
| Calls per Hour | ≥ 45 | ≥ 45 |
| Daily Calls | 315 | 315 |
| Success Rate | ≥ 17% | ≥ 20% |
| Work Time Efficiency | ≥ 70% of clocked time on dialer | ≥ 70% |

To change benchmarks, edit `index.html` line:
```javascript
const B = { rm:{cph:45,sr:0.17}, fancy:{cph:45,sr:0.20} };
```

---

## 🛠 Troubleshooting

**Dashboard shows "Could not load data/weekly_data.json"**
→ GitHub Pages might still be setting up. Wait 2–3 minutes and refresh.

**GitHub Actions workflow fails**
→ Check the Actions tab for the error log. Most common cause: wrong secret names.

**DialFire returns no agents**
→ The API endpoint may need adjusting. Open `scripts/fetch_dialfire.py` and check the `api_get()` call — print the raw response to see what fields DialFire returns for your account.

**Agent names don't match ConnectTeams**
→ The name matching is flexible (partial match). If it still fails, make sure names are spelled the same in both systems.

---

## 📁 File Structure

```
re-dashboard/
├── index.html                          ← The dashboard (GitHub Pages serves this)
├── data/
│   └── weekly_data.json                ← Auto-updated by GitHub Actions
├── scripts/
│   └── fetch_dialfire.py               ← DialFire API fetcher
├── .github/
│   └── workflows/
│       └── update-data.yml             ← Daily automation schedule
└── README.md                           ← This file
```
