# CIQ Azure Cost Monitor (local)

A small Python project that pulls Azure cost data for the CIQ + Forecaster
subscriptions via the Cost Management API, stores it in a local DuckDB
file, and surfaces it through a Streamlit dashboard.

Everything runs **locally**. No cloud resources are created.

Subscriptions in scope (subscription IDs live in `.env`):
- `ciq-dev` / `ciq-staging` / `ciq-prod` — environments: `dev`, `staging`, `prod`
- `forecaster-dev` — environment: `forecaster-dev`

## Layout

```
cost-monitor/
  config.py          # SUBSCRIPTIONS list + ingestion windows + API settings
  azure_client.py    # DefaultAzureCredential + Cost Management REST client
  transform.py       # service_family / MC_* AKS attribution + row builders
  db.py              # DuckDB schema + idempotent upsert
  ingest.py          # CLI entrypoint
  dashboard.py       # Streamlit dashboard (reads DuckDB only)
  tests/             # pytest suite for transform.py
  requirements.txt
  .env.example
```

## Setup

```bash
cd forecast-ml/cost-monitor
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
# Open .env and fill in the four *_SUB_ID values (ask a teammate if needed).
```

The four `*_SUB_ID` env vars are **required** — `config.py` raises on
startup if any of them is missing. No subscription IDs are hard-coded in
source.

## Azure auth

The client uses `azure-identity`'s `DefaultAzureCredential`. In order:

1. **Service principal env vars** — `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
   `AZURE_CLIENT_SECRET`. Set all three to use SP auth (required for
   unattended runs like cron).
2. **`az login` session** — simplest for interactive local use.

```bash
az login --scope https://management.azure.com/.default
az account list --query "[?starts_with(name, 'ciq') || name=='forecaster-dev']" -o table
```

You need **Cost Management Reader** (or higher) on each subscription. The
tool never tries to create roles.

If `ingest.py` exits with a 401/403 message, re-authenticate (`az login`)
or ask whoever owns the subscriptions to grant the role.

## First run

```bash
python ingest.py
```

What this does, per subscription:
1. Validates Cost Management access (cheap query against yesterday).
2. Pulls **daily AmortizedCost** for the **last 90 days** grouped by
   `ServiceName / MeterSubCategory / ResourceId`.
3. Pulls **monthly AmortizedCost** from **2023-01-01 → today**, chunked
   into ≤364-day windows (Cost Management's Custom-timeframe limit).
4. Calls the **Forecast** endpoint for projected month-end total.
5. Upserts everything into **`cost-monitor/costs.duckdb` on your local
   disk** — re-runs are idempotent. This is the source of truth.
6. If `AZURE_BLOB_SAS_URL` is set, mirrors the fresh local file up to
   Azure Blob Storage so a hosted dashboard can read it. Skipped silently
   when the URL isn't configured (purely local mode).

The ingestion windows are fixed — no `--backfill` or month-count flags.
Expect ~5–10 min total on first run because Azure's per-subscription
throttle (~5 req/min) forces ~13s between calls plus adaptive backoff
when 429s show up.

Other useful invocations:

```bash
python ingest.py --only dev              # one environment
python ingest.py --only forecaster-dev   # the forecaster sub
python ingest.py --skip-forecast         # skip the projection call
python ingest.py -v                      # verbose
```

## Launch the dashboard

```bash
streamlit run dashboard.py
```

Opens at `http://localhost:8501`. The dashboard always reads from a
local `costs.duckdb` file — it never calls the Cost Management API.

**Where that local file comes from depends on whether `AZURE_BLOB_SAS_URL`
is set**:

- **Set** (deployed / shared use) — on startup the dashboard downloads
  the blob's copy and **overwrites** the local `costs.duckdb`. Blob is
  the source of truth; the local file is just a session-scoped cache.
  This is what makes the laptop dashboard and the Streamlit Cloud
  dashboard show the same numbers.
- **Unset** (pure local mode) — the dashboard reads whatever
  `python ingest.py` wrote last. Blob isn't touched.

Either way, **`python ingest.py` writes locally first** and only then
mirrors to blob (step 5–6 above). If you've just run ingest and the
dashboard cache is stale, press `R` in the browser to clear it.

### Restrict access (username + password)

Set both env vars to gate every visitor behind a login form:

```bash
# in .env (local) or Streamlit Cloud → Settings → Secrets
APP_USERNAME=team
APP_PASSWORD=<something-strong>
```

When set, the dashboard renders only a login form until a viewer enters
matching credentials. The check is constant-time (`secrets.compare_digest`)
so it doesn't leak via response timing. A **Sign out** button appears in
the sidebar once authenticated.

If either var is empty (or unset), the gate is bypassed — handy for
local dev. **Don't ship to Streamlit Cloud without both set**, or the
dashboard is public to anyone with the URL.

Single shared credential is fine for a small team; rotate by editing
the secret and re-deploying. If you outgrow this and need per-user
accounts, swap in `streamlit-authenticator` (about 30 LOC of changes
in this file).

### Sections

- **Summary** — three side-by-side tables (CIQ, Forecaster, Combined),
  each with: Environment, MTD, vs prior MTD %, Projected month-end,
  Total {last year}, Avg/mo {last year}, Total {current year} (closed
  months only), Avg/mo {current year}. The current in-progress month
  is excluded from both numerator and denominator of the YTD figures
  so the run rate isn't deflated; the MTD column shows it instead.
- **Daily cost — last 90 days** — stacked bar by `service_family`, total
  USD label on top of each day.
- **Monthly cost — Jan 2023 onward** — one chart per environment
  (`dev`, `staging`, `prod`, `forecaster-dev`) plus a combined total,
  all stacked by `service_family` with totals labelled.
- **Resources by spend** — all resource-attributed rows in the
  selected window, sorted descending.
- **Databricks breakdown** by `MeterSubCategory` (All-Purpose / Jobs /
  SQL / …).
- **Anomalies** — any `(environment, service_family)` whose latest-day
  spend exceeds 2× its trailing 7-day average.

A caption shows the last successful ingest timestamp and a reminder
that Azure cost data lags 8–24 hours.

## Adding / changing a subscription

Edit `.env` to add an `<NAME>_SUB_ID=...` line, then add the matching
`Subscription(...)` entry in `config.py`:

```python
# config.py
SUBSCRIPTIONS: list[Subscription] = [
    ...,
    Subscription(
        name="forecaster-staging",
        id=_required_env("FORECASTER_STAGING_SUB_ID"),
        environment="forecaster-staging",
    ),
]
```

To make the new env show up in the dashboard, also add it to
`CIQ_ENVS` or `FORECASTER_ENVS` and (optionally) `ENV_COLORS` in
`dashboard.py`.

## Deploy to Streamlit Community Cloud

Hosts the dashboard at a public Streamlit Cloud URL, restricted to a
list of approved GitHub accounts. Architecture:

```
┌──────────────┐    upload    ┌──────────────────┐    download    ┌────────────────────┐
│ ingest.py    │ ──────────► │  Azure Blob      │ ◄───────────── │ Streamlit Cloud    │
│ (your laptop │              │  costs.duckdb    │                │ runs dashboard.py  │
│  / cron)     │              │  (SAS-protected) │                │ Read-only          │
└──────────────┘              └──────────────────┘                └────────────────────┘
       │
       ▼
Azure Cost Management API
```

### One-time setup

**1. Have someone with Azure write access create the blob target.**

You said you have read-only Azure, so this part needs a teammate:

- Storage account: an existing `saciq*` or `ciqinfrastructuresa` is
  fine, or a new one.
- Container: `cost-monitor` (or any name — match it in the URL).
- Blob name: `costs.duckdb`.
- Generate a **Service SAS** scoped to that single blob:
  - Permissions: `Read`, `Write`, `Create`
  - Expiry: 1 year (longest reasonable for unattended use)
  - Allowed protocols: `HTTPS only`
- Copy the full URL — it'll look like:
  `https://saciqdev.blob.core.windows.net/cost-monitor/costs.duckdb?sv=2024-...&se=2027-...&sp=rwc&sig=...`

**2. Wire the URL into your local `.env` and run ingest:**

```bash
echo 'AZURE_BLOB_SAS_URL=https://...full SAS URL...' >> .env
python ingest.py
# Look for "Uploaded costs.duckdb to Azure Blob." in the output.
```

**3. Push cost-monitor to a GitHub repo** that Streamlit Cloud can reach.
If `forecast-ml` is private and you don't want the wider org to see
this, push to a **separate private repo** (e.g. `cost-monitor`) with
just this folder's contents.

**4. Deploy on Streamlit Cloud:**

- Go to https://share.streamlit.io → **New app**
- Repo: select the one from step 3
- Branch: `main` (or your dashboard branch)
- Main file path: `dashboard.py` (or `cost-monitor/dashboard.py` if
  inside a subfolder)
- Click **Advanced settings** → paste the contents of
  `.streamlit/secrets.toml.example` with real values into the
  **Secrets** box. At minimum:
  ```toml
  CIQ_DEV_SUB_ID         = "..."
  CIQ_STAGING_SUB_ID     = "..."
  CIQ_PROD_SUB_ID        = "..."
  FORECASTER_DEV_SUB_ID  = "..."
  AZURE_BLOB_SAS_URL     = "https://...full SAS URL..."
  ```
- Click **Deploy**

**5. Restrict viewers to the CIQ team:**

In the deployed app → **Settings** → **Sharing**:
- Change visibility to **Specific people**
- Add the GitHub email or username of each teammate who should see it

That's enforced by Streamlit Cloud before any viewer reaches the app.

### What runs where

- **`ingest.py`** runs on your laptop (or a cron). Writes
  `costs.duckdb` locally, then uploads to blob.
- **`dashboard.py`** runs on Streamlit Cloud. On the first request after
  app startup it downloads `costs.duckdb` from blob, caches the file
  in the ephemeral container, and serves queries from it. The cache
  is wiped on container restart (Streamlit Cloud restarts apps
  periodically, so freshness lags ingest by at most ~1 day of idle
  time + 5-min Streamlit data cache).

### Refreshing the dashboard

After running `python ingest.py` locally, the new `costs.duckdb` is
in the blob immediately. To see it in the cloud:
1. Click **Manage app** (bottom right) → **Reboot**, or
2. Hit `R` in the browser to clear Streamlit's 5-min data cache (only
   helps if the container already has the new file — usually a reboot
   is cleaner).

### SAS rotation reminders

The SAS token has an expiry. Set a calendar reminder ~2 weeks before
the date in the URL's `&se=YYYY-MM-DDT...` parameter. When it expires:
1. Generate a new SAS in the same container
2. Update `AZURE_BLOB_SAS_URL` in your local `.env` *and* in Streamlit
   Cloud → Settings → Secrets
3. The next ingest run + app reboot picks up the new token

## Scheduling

### macOS / Linux (cron)

```cron
0 * * * * cd /Users/iman/Desktop/Source/ciq\&forecaster/forecast-ml/cost-monitor && /Users/iman/Desktop/Source/ciq\&forecaster/forecast-ml/cost-monitor/.venv/bin/python ingest.py >> /tmp/cost-monitor.log 2>&1
```

Notes:
- Cron does **not** inherit your shell env. Put the `AZURE_TENANT_ID` /
  `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` triplet in `.env` so
  `python-dotenv` picks them up.
- `az login` tokens expire under inactivity, so for unattended runs
  **prefer the service-principal env vars** over `az login`.

### Windows (Task Scheduler)

Create a Basic Task → Daily, repeating every 1 hour:

- Program: `C:\path\to\cost-monitor\.venv\Scripts\python.exe`
- Arguments: `ingest.py`
- Start in: `C:\path\to\cost-monitor`

Same auth caveat: use SP env vars in `.env` for unattended runs.

## Tests

```bash
pytest
```

34 tests covering the attribution helpers in `transform.py`:
- `infer_environment` — subscription name regex + resource-id fallbacks
- `infer_service_family` — bucketing
- **The MC_* AKS detection** (easy thing to get wrong — AKS node VMs
  are billed as "Virtual Machines" under an `MC_*` resource group, not
  under "Azure Kubernetes Service"; missing this badly understates AKS
  totals)
- `is_databricks_managed` for `databricks-rg` / `dbstorage*`
- `UsageDate` parsing (`20260527` → `date(2026, 5, 27)`)
- `build_daily_row` end-to-end, including a non-CIQ environment

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `RuntimeError: Missing required env var 'CIQ_DEV_SUB_ID'` | `.env` missing or var unset | `cp .env.example .env` and fill in the four `*_SUB_ID` values |
| `Auth error: ... 401`/`403` | `az login` expired or no Cost Management Reader role | `az login --scope https://management.azure.com/.default`, or have the role granted |
| `Cost Management API returned 429` | Hit Azure's per-sub throttle | `ingest.py` ratchets the per-request floor up adaptively and retries with backoff; if it still fails after several minutes, wait and re-run |
| `Cost Management API returned 400` (forecast) | Not enough history for forecast on a fresh sub | Expected — daily/monthly data still ingests; forecast tile shows "—" |
| Dashboard says `costs.duckdb not found` | Haven't ingested yet | `python ingest.py` |
| Numbers look low for AKS | Node VMs in MC_* RGs not bucketed | Confirm the MC_* rule in `transform.py:infer_service_family` is firing (the test suite verifies this) |
| `Could not set lock on file costs.duckdb` | Dashboard holds a read connection | Stop Streamlit, run ingest, restart Streamlit. (Dashboard releases the lock between queries, but there's a small overlap window.) |

## Notes

- Currency is **USD** throughout; the ingestor uses `AmortizedCost`.
- Cost data lags 8–24 hours. The dashboard labels figures accordingly —
  don't treat them as real-time.
- The dashboard caches DuckDB reads for 5 minutes; restart Streamlit
  (or press `R` in the browser) after a fresh ingest to bust the cache.
- This project lives under `forecast-ml/cost-monitor/` but is **not**
  part of the `forecaster` / `ciq` Poetry wheel — it's a standalone
  ops tool that shares the repo for convenience.
