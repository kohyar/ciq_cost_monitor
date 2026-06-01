"""Static configuration for the cost-monitor project.

Subscription IDs are loaded from environment variables (typically via .env).
The four required vars are CIQ_DEV_SUB_ID / CIQ_STAGING_SUB_ID /
CIQ_PROD_SUB_ID / FORECASTER_DEV_SUB_ID.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Subscription:
    name: str
    id: str
    environment: str


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required env var {name!r}. "
            "Set it in cost-monitor/.env (see .env.example)."
        )
    return value


SUBSCRIPTIONS: list[Subscription] = [
    Subscription(name="ciq-dev", id=_required_env("CIQ_DEV_SUB_ID"), environment="ciq-dev"),
    Subscription(name="ciq-staging", id=_required_env("CIQ_STAGING_SUB_ID"), environment="ciq-staging"),
    Subscription(name="ciq-prod", id=_required_env("CIQ_PROD_SUB_ID"), environment="ciq-prod"),
    Subscription(
        name="forecaster-dev",
        id=_required_env("FORECASTER_DEV_SUB_ID"),
        environment="forecaster-dev",
    ),
]

# Cost Management API
COST_API_VERSION = "2023-11-01"
COST_API_BASE = "https://management.azure.com"
MANAGEMENT_SCOPE = "https://management.azure.com/.default"

# Fixed ingestion windows. The CLI no longer exposes these as flags — both
# values are deliberately the same on every run.
DAILY_LOOKBACK_DAYS = 90
MONTHLY_START_DATE = date(2023, 1, 1)

# Rate-limit handling. Subscription-scope Cost Management is throttled to
# ~5 requests/min in practice (Azure docs say 15 req/min but the live limit
# is lower for most subscriptions). We start with a 13s floor (≈4.6 req/min)
# and ratchet it up adaptively each time we see a 429.
MAX_RETRIES = 8
RETRY_BASE_DELAY_SECONDS = 5.0
RETRY_MAX_DELAY_SECONDS = 120.0
INTER_REQUEST_DELAY_SECONDS = 13.0
ADAPTIVE_DELAY_MAX_SECONDS = 60.0  # ceiling for the adaptive floor

# Currency. Azure bills CIQ in USD; if this ever changes the dashboard label
# also needs to be updated.
EXPECTED_CURRENCY = "USD"

# Local storage.
PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("COST_DB_PATH", PROJECT_ROOT / "costs.duckdb"))

# Optional Azure Blob backing store. Set to a full SAS URL like
#   https://<account>.blob.core.windows.net/<container>/<blob>?<sas-query>
# When set:
#   * ingest.py uploads costs.duckdb to the blob after each successful run
#   * dashboard.py downloads it on startup when the local file is missing
#     (so the same dashboard.py works locally AND on Streamlit Cloud).
AZURE_BLOB_SAS_URL = os.getenv("AZURE_BLOB_SAS_URL", "").strip() or None

# Databricks SQL warehouses, used to pull `system.billing.usage` for
# per-job DBU attribution. Two config patterns are supported:
#
#   1. PER-WORKSPACE (preferred): set DATABRICKS_<NAME>_HOST /
#      _HTTP_PATH / _TOKEN for each workspace you want to ingest from.
#      Workspaces with all three set show up in DATABRICKS_WORKSPACES.
#      Use this when your workspaces have separate metastores.
#
#   2. LEGACY single-workspace: set DATABRICKS_HOST / DATABRICKS_HTTP_PATH
#      / DATABRICKS_TOKEN. Treated as one workspace named "default". If
#      your 4 workspaces share one metastore, one connection is enough —
#      system.billing.usage rows carry workspace_id so all 4 are visible.
#
# These costs are NEVER summed into Azure totals — they're a drill-down
# of the existing "databricks" service family rollup.

@dataclass(frozen=True)
class DatabricksWorkspace:
    name: str       # friendly label used in logs and the summary table
    host: str       # adb-XXXXXXXXXXXXXXXX.NN.azuredatabricks.net
    http_path: str  # /sql/1.0/warehouses/...
    token: str      # PAT (or rotate to OAuth M2M later)


# Workspace_id (Azure Databricks workspace id) → environment label the
# dashboard already uses for filtering. Add new workspaces here when
# spinning them up. workspace_id appears in every row of
# system.billing.usage so this mapping lets the dashboard's env filter
# also apply to the Databricks-jobs section.
DATABRICKS_WORKSPACE_TO_ENV: dict[str, str] = {
    "8346278133268666": "ciq-dev",
    "4272234136902058": "ciq-staging",
    "3064739478924637": "ciq-prod",
    "8911757934082442": "forecaster-dev",
}


def _load_databricks_workspaces() -> list[DatabricksWorkspace]:
    workspaces: list[DatabricksWorkspace] = []
    # Per-workspace pattern first.
    for prefix, label in (
        ("DATABRICKS_CIQ_DEV", "ciq-dev"),
        ("DATABRICKS_CIQ_STAGING", "ciq-staging"),
        ("DATABRICKS_CIQ_PROD", "ciq-prod"),
        ("DATABRICKS_FORECASTER_DEV", "forecaster-dev"),
    ):
        host = os.getenv(f"{prefix}_HOST", "").strip()
        http_path = os.getenv(f"{prefix}_HTTP_PATH", "").strip()
        token = os.getenv(f"{prefix}_TOKEN", "").strip()
        if host and http_path and token:
            workspaces.append(
                DatabricksWorkspace(name=label, host=host, http_path=http_path, token=token)
            )

    # Legacy single-workspace pattern (kept so an existing .env doesn't
    # break). Only used when no per-workspace creds are present.
    if not workspaces:
        host = os.getenv("DATABRICKS_HOST", "").strip()
        http_path = os.getenv("DATABRICKS_HTTP_PATH", "").strip()
        token = os.getenv("DATABRICKS_TOKEN", "").strip()
        if host and http_path and token:
            workspaces.append(
                DatabricksWorkspace(name="default", host=host, http_path=http_path, token=token)
            )

    return workspaces


DATABRICKS_WORKSPACES: list[DatabricksWorkspace] = _load_databricks_workspaces()

# Window for the Databricks job query. `system.billing.usage` carries
# ~365 days of history; we ask for the full year so the dashboard's
# date picker can range freely without re-querying when the user
# scrolls back in time.
DATABRICKS_LOOKBACK_DAYS = 365

# SKUs to exclude from the Databricks-jobs dashboard section. Useful for
# pass-through line items billed separately (e.g. Anthropic's API charged
# through Databricks but tracked elsewhere) that would double-count if
# treated as Databricks compute.
DATABRICKS_EXCLUDED_SKUS: set[str] = {
    "PREMIUM_ANTHROPIC_MODEL_SERVING",
}


def databricks_configured() -> bool:
    return bool(DATABRICKS_WORKSPACES)
