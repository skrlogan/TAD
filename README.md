# Tactical Monitor

A deterministic technical monitoring dashboard for ETF watchlists.

## Setup

### 1. Create a GitHub repository

- Go to [github.com](https://github.com) and create a new **public** repository
- Name it anything, e.g. `tactical-monitor`

### 2. Add the files

Upload these files to the root of your repository:

```
dashboard.html
fetch_stooq.py
.github/
  workflows/
    fetch-data.yml
```

The easiest way is to drag-and-drop them into the GitHub web UI after creating the repo.

### 3. Enable GitHub Pages

- Go to **Settings → Pages**
- Under **Source**, select **Deploy from a branch**
- Choose **main** branch, **/ (root)** folder
- Click **Save**

Your dashboard will be live at:
`https://<your-username>.github.io/<repo-name>/dashboard.html`

### 4. Run the first data fetch manually

GitHub Actions will run automatically on schedule, but to get data immediately:

- Go to **Actions** tab in your repository
- Click **Fetch Stooq Data**
- Click **Run workflow → Run workflow**

This fetches all 14 symbols and commits the data files to your repo.
The dashboard will then load data instantly from these files.

### 5. Automatic schedule

After setup, GitHub Actions runs automatically:
- **Monday–Friday at 21:00 UTC** (5pm ET)
- **Sunday at 20:00 UTC** (4pm ET)

No further action needed.

---

## How it works

- **Data**: `fetch_stooq.py` fetches OHLCV CSV data from Stooq server-side
  (no CORS issues) and saves it as JSON files under `data/`
- **Dashboard**: reads from `data/<symbol>_<freq>.json` — instant, no network
  dependency at runtime
- **Manual fetch**: the per-row **Fetch** button still attempts a live fetch
  via public CORS proxies as a fallback
- **Upload CSV**: if all else fails, download a CSV from stooq.com manually
  and upload it directly in the dashboard

## Adding symbols

Edit the `SYMBOLS` list in `fetch_stooq.py` and the default watchlist in
`dashboard.html`. Then re-run the workflow manually to fetch data for new symbols.

## Data freshness

The status bar shows when data was last fetched by GitHub Actions.
Weekly data only needs to update once a week, so the Sunday schedule is sufficient
for the default weekly mode.
