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
    Subscription(name="ciq-dev", id=_required_env("CIQ_DEV_SUB_ID"), environment="dev"),
    Subscription(name="ciq-staging", id=_required_env("CIQ_STAGING_SUB_ID"), environment="staging"),
    Subscription(name="ciq-prod", id=_required_env("CIQ_PROD_SUB_ID"), environment="prod"),
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
