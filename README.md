# OCEANUS — Maritime Intelligence Dashboard

Live maritime tracking dashboard hosted on GitHub Pages, updated automatically via GitHub Actions.

## Setup

### 1. Fork / create the repository

Push these files to your GitHub repo:
```
maritime-tracker.html        ← the dashboard
fetch_ais.py                 ← data fetcher (runs via Actions)
data/vessels.json            ← auto-updated vessel positions
data/alerts.json             ← auto-updated NGA MSI alerts
.github/workflows/fetch-ais.yml  ← the Actions workflow
```

### 2. Add your AIS API key as a secret

Go to **Settings → Secrets and variables → Actions → New repository secret**:

| Name | Value |
|------|-------|
| `AIS_API_KEY` | Your aisstream.io API key |

Get a free key at [aisstream.io](https://aisstream.io).

### 3. Enable GitHub Pages

Go to **Settings → Pages → Source: Deploy from a branch → main → / (root)**.

Your dashboard will be live at `https://yourusername.github.io/your-repo/maritime-tracker.html`

### 4. Enable GitHub Actions

Go to **Actions → Enable workflows** if not already enabled.

The `fetch-ais.yml` workflow runs every 2 minutes, collects 90 seconds of AIS data,
and commits `data/vessels.json` + `data/alerts.json` to the repo.

## How it works

```
GitHub Actions (every 2 min)
  └─ fetch_ais.py
       ├─ Connects to aisstream.io WebSocket (90s)
       ├─ Writes data/vessels.json  →  GitHub Pages
       └─ Fetches NGA MSI alerts
            └─ Writes data/alerts.json  →  GitHub Pages

Browser (maritime-tracker.html)
  ├─ Fetches data/vessels.json  →  renders ships on canvas map
  └─ Fetches data/alerts.json   →  shows ⚠ Alerts modal
```

## Notes

- GitHub Actions free tier: 2,000 minutes/month. At 2-min intervals = ~21,600 runs/month — exceeds free tier. Increase the cron interval to `*/5 * * * *` (every 5 min) for ~8,640 runs/month which fits comfortably.
- The workflow collects positions for 90 seconds so each snapshot has good global coverage.
- Vessel history trails are dead-reckoned (speed × heading × time) — not actual recorded positions.
