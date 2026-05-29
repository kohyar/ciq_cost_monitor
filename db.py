"""DuckDB persistence layer.

Local file is the source of truth. When AZURE_BLOB_SAS_URL is set in
config, the file is mirrored to/from Azure Blob Storage so the same
costs.duckdb can be produced locally and consumed by a Streamlit Cloud
app that has no persistent disk.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Sequence

import duckdb

from config import AZURE_BLOB_SAS_URL, DB_PATH

log = logging.getLogger(__name__)


COST_COLUMNS: tuple[str, ...] = (
    "date",
    "subscription_id",
    "environment",
    "service_family",
    "service_name",
    "meter_subcategory",
    "resource_group",
    "resource_id",
    "resource_name",
    "cost",
    "currency",
    "ingested_at",
)


def connect(db_path: Path | str = DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=read_only)


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS azure_costs (
            date              DATE      NOT NULL,
            subscription_id   TEXT      NOT NULL,
            environment       TEXT,
            service_family    TEXT,
            service_name      TEXT,
            meter_subcategory TEXT,
            resource_group    TEXT,
            resource_id       TEXT,
            resource_name     TEXT,
            cost              DOUBLE    NOT NULL,
            currency          TEXT      NOT NULL,
            ingested_at       TIMESTAMP NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS azure_costs_monthly (
            month_start       DATE      NOT NULL,
            subscription_id   TEXT      NOT NULL,
            environment       TEXT,
            service_family    TEXT,
            service_name      TEXT,
            cost              DOUBLE    NOT NULL,
            currency          TEXT      NOT NULL,
            ingested_at       TIMESTAMP NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS azure_forecast (
            subscription_id      TEXT      NOT NULL,
            environment          TEXT      NOT NULL,
            projected_month_end  DOUBLE    NOT NULL,
            as_of                DATE      NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_runs (
            subscription_id  TEXT      NOT NULL,
            kind             TEXT      NOT NULL,
            started_at       TIMESTAMP NOT NULL,
            finished_at      TIMESTAMP,
            row_count        BIGINT,
            status           TEXT,
            error            TEXT
        )
        """
    )


def upsert_daily(
    con: duckdb.DuckDBPyConnection,
    rows: Sequence[dict],
) -> int:
    """Idempotent upsert: delete the (date, subscription_id, resource_id,
    meter_subcategory) tuples being written, then insert the new batch."""
    if not rows:
        return 0

    keys = sorted(
        {
            (
                r["date"],
                r["subscription_id"],
                r.get("resource_id") or "",
                r.get("meter_subcategory") or "",
            )
            for r in rows
        }
    )
    con.execute("CREATE TEMPORARY TABLE _to_replace (date DATE, subscription_id TEXT, resource_id TEXT, meter_subcategory TEXT)")
    try:
        con.executemany(
            "INSERT INTO _to_replace VALUES (?, ?, ?, ?)",
            [list(k) for k in keys],
        )
        con.execute(
            """
            DELETE FROM azure_costs c
            USING _to_replace r
            WHERE c.date = r.date
              AND c.subscription_id = r.subscription_id
              AND COALESCE(c.resource_id, '') = r.resource_id
              AND COALESCE(c.meter_subcategory, '') = r.meter_subcategory
            """
        )
    finally:
        con.execute("DROP TABLE _to_replace")

    payload = [[row.get(col) for col in COST_COLUMNS] for row in rows]
    placeholders = ", ".join(["?"] * len(COST_COLUMNS))
    con.executemany(
        f"INSERT INTO azure_costs ({', '.join(COST_COLUMNS)}) VALUES ({placeholders})",
        payload,
    )
    return len(rows)


def upsert_monthly(
    con: duckdb.DuckDBPyConnection,
    rows: Sequence[dict],
) -> int:
    if not rows:
        return 0

    keys = sorted(
        {
            (r["month_start"], r["subscription_id"], r.get("service_name") or "")
            for r in rows
        }
    )
    con.execute("CREATE TEMPORARY TABLE _to_replace_m (month_start DATE, subscription_id TEXT, service_name TEXT)")
    try:
        con.executemany("INSERT INTO _to_replace_m VALUES (?, ?, ?)", [list(k) for k in keys])
        con.execute(
            """
            DELETE FROM azure_costs_monthly m
            USING _to_replace_m r
            WHERE m.month_start = r.month_start
              AND m.subscription_id = r.subscription_id
              AND COALESCE(m.service_name, '') = r.service_name
            """
        )
    finally:
        con.execute("DROP TABLE _to_replace_m")

    cols = (
        "month_start",
        "subscription_id",
        "environment",
        "service_family",
        "service_name",
        "cost",
        "currency",
        "ingested_at",
    )
    payload = [[row.get(c) for c in cols] for row in rows]
    placeholders = ", ".join(["?"] * len(cols))
    con.executemany(
        f"INSERT INTO azure_costs_monthly ({', '.join(cols)}) VALUES ({placeholders})",
        payload,
    )
    return len(rows)


def replace_forecast(
    con: duckdb.DuckDBPyConnection,
    rows: Sequence[dict],
) -> int:
    """Forecast is a small snapshot — wipe & rewrite for the listed subscriptions."""
    if not rows:
        return 0
    sub_ids = sorted({r["subscription_id"] for r in rows})
    placeholders = ", ".join(["?"] * len(sub_ids))
    con.execute(
        f"DELETE FROM azure_forecast WHERE subscription_id IN ({placeholders})",
        sub_ids,
    )
    cols = ("subscription_id", "environment", "projected_month_end", "as_of")
    payload = [[r.get(c) for c in cols] for r in rows]
    ph = ", ".join(["?"] * len(cols))
    con.executemany(
        f"INSERT INTO azure_forecast ({', '.join(cols)}) VALUES ({ph})",
        payload,
    )
    return len(rows)


def record_run(
    con: duckdb.DuckDBPyConnection,
    subscription_id: str,
    kind: str,
    started_at: datetime,
    finished_at: datetime | None,
    row_count: int | None,
    status: str,
    error: str | None,
) -> None:
    con.execute(
        """
        INSERT INTO ingest_runs (subscription_id, kind, started_at, finished_at, row_count, status, error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [subscription_id, kind, started_at, finished_at, row_count, status, error],
    )


def last_successful_ingest(con: duckdb.DuckDBPyConnection) -> datetime | None:
    row = con.execute(
        """
        SELECT MAX(finished_at) FROM ingest_runs WHERE status = 'success'
        """
    ).fetchone()
    return row[0] if row and row[0] else None


# ---- Azure Blob mirror ----------------------------------------------------


_DEFAULT_BLOB_FILENAME = "costs.duckdb"


def _normalize_blob_url(url: str, filename: str = _DEFAULT_BLOB_FILENAME) -> str:
    """Append `filename` to the URL path when the SAS points at a directory.

    Detection: if the last path segment has no '.' it's treated as a
    directory and the filename is appended before the '?' query string.
    Already-leaf URLs (ending in '*.duckdb' etc.) are returned unchanged.
    """
    if "?" in url:
        base, query = url.split("?", 1)
        query = "?" + query
    else:
        base, query = url, ""
    last = base.rstrip("/").rsplit("/", 1)[-1]
    if "." not in last:
        base = base.rstrip("/") + "/" + filename
    return base + query


def _blob_client():
    """Return a BlobClient for AZURE_BLOB_SAS_URL, or None if not configured."""
    if not AZURE_BLOB_SAS_URL:
        return None
    # Imported lazily so the package is only needed when the feature is used.
    from azure.storage.blob import BlobClient
    return BlobClient.from_blob_url(_normalize_blob_url(AZURE_BLOB_SAS_URL))


def upload_db_to_blob(local_path: Path | str = DB_PATH) -> bool:
    """Upload the local DuckDB file to Azure Blob Storage. No-op if the
    blob URL isn't configured. Returns True on success.
    """
    client = _blob_client()
    if client is None:
        return False
    local = Path(local_path)
    if not local.exists():
        raise FileNotFoundError(f"Cannot upload — {local} does not exist")
    size_mb = local.stat().st_size / 1024 / 1024
    log.info("Uploading %s (%.1f MB) to blob …", local.name, size_mb)
    with open(local, "rb") as fh:
        client.upload_blob(fh, overwrite=True)
    log.info("Upload complete")
    return True


def download_db_from_blob(local_path: Path | str = DB_PATH) -> bool:
    """Download the DuckDB file from Azure Blob Storage to `local_path`.
    No-op if the blob URL isn't configured. Returns True on success.
    """
    client = _blob_client()
    if client is None:
        return False
    local = Path(local_path)
    local.parent.mkdir(parents=True, exist_ok=True)
    log.info("Downloading costs.duckdb from blob → %s", local)
    with open(local, "wb") as fh:
        downloader = client.download_blob()
        fh.write(downloader.readall())
    log.info("Download complete (%.1f MB)", local.stat().st_size / 1024 / 1024)
    return True


def ensure_local_db(local_path: Path | str = DB_PATH) -> Path:
    """Make sure a usable DuckDB file is at `local_path`.

    Blob is the single source of truth when AZURE_BLOB_SAS_URL is set:
    we always download (overwriting any local copy) so the laptop and
    Streamlit Cloud see exactly the same data. Streamlit caches this
    via @st.cache_resource so it only fires once per app session.

    If no blob URL is configured, falls back to the existing local file.
    """
    local = Path(local_path)
    if AZURE_BLOB_SAS_URL:
        download_db_from_blob(local)
        return local
    if local.exists():
        return local
    raise FileNotFoundError(
        f"{local} not found and AZURE_BLOB_SAS_URL is not set. "
        "Run `python ingest.py` locally first, or configure the blob URL."
    )
