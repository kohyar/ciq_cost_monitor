"""Tiny wrapper around the Databricks SQL connector.

Used to pull per-job DBU consumption + dollar cost from `system.billing.usage`
so the dashboard can show a Databricks-side drill-down of what's already
included in the Azure "databricks" service-family rollup.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date, datetime

from config import (
    DATABRICKS_LOOKBACK_DAYS,
    DATABRICKS_WORKSPACES,
    DatabricksWorkspace,
)

log = logging.getLogger(__name__)


class DatabricksAuthError(RuntimeError):
    """Raised when no Databricks SQL credentials are configured."""


@contextmanager
def _connection(workspace: DatabricksWorkspace):
    """Open a Databricks SQL warehouse connection for a single workspace."""
    # Lazy import so the rest of the project doesn't need the connector
    # installed when this feature isn't in use.
    from databricks import sql

    con = sql.connect(
        server_hostname=workspace.host,
        http_path=workspace.http_path,
        access_token=workspace.token,
    )
    try:
        yield con
    finally:
        con.close()


# Pre-joined query: usage × list_prices × latest job name. Aggregated by
# (date, workspace, job, cluster, warehouse, sku) so a re-run replaces the
# same rows. `warehouse_id` covers serverless SQL warehouse usage that has
# no job_id or cluster_id (otherwise these rows show as "interactive-unknown").
_QUERY_WITH_JOB_NAMES = """
WITH usage_agg AS (
    SELECT
        u.usage_date,
        u.workspace_id,
        u.usage_metadata.job_id       AS job_id,
        u.usage_metadata.cluster_id   AS cluster_id,
        u.usage_metadata.warehouse_id AS warehouse_id,
        u.sku_name,
        SUM(u.usage_quantity)                            AS total_dbus,
        SUM(u.usage_quantity * lp.pricing.default)       AS cost_usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices lp
        ON u.sku_name = lp.sku_name
       AND u.usage_date BETWEEN lp.price_start_time
                            AND COALESCE(lp.price_end_time, CURRENT_DATE())
    WHERE u.usage_date >= DATE_SUB(CURRENT_DATE(), %(lookback)s)
    GROUP BY u.usage_date, u.workspace_id, u.usage_metadata.job_id,
             u.usage_metadata.cluster_id, u.usage_metadata.warehouse_id,
             u.sku_name
),
latest_job_name AS (
    SELECT
        CAST(job_id AS STRING) AS job_id,
        name,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
)
SELECT
    ua.usage_date,
    ua.workspace_id,
    ua.job_id,
    j.name AS job_name,
    ua.cluster_id,
    COALESCE(
        j.name,
        CASE
            WHEN ua.job_id IS NOT NULL       THEN CONCAT('(unnamed job ', ua.job_id, ')')
            WHEN ua.cluster_id IS NOT NULL   THEN CONCAT('interactive-', ua.cluster_id)
            WHEN ua.warehouse_id IS NOT NULL THEN CONCAT('sql-warehouse-', ua.warehouse_id)
            ELSE CONCAT('sku-', ua.sku_name)
        END
    ) AS workload,
    ua.sku_name,
    ua.total_dbus,
    ua.cost_usd
FROM usage_agg ua
LEFT JOIN latest_job_name j
    ON CAST(ua.job_id AS STRING) = j.job_id AND j.rn = 1
"""

# Fallback when `system.lakeflow.jobs` isn't available — same shape, NULL
# job_name, workload uses the same precedence sans real names.
_QUERY_WITHOUT_JOB_NAMES = """
SELECT
    u.usage_date,
    u.workspace_id,
    u.usage_metadata.job_id     AS job_id,
    CAST(NULL AS STRING)         AS job_name,
    u.usage_metadata.cluster_id AS cluster_id,
    CASE
        WHEN u.usage_metadata.job_id IS NOT NULL
            THEN CONCAT('(unnamed job ', u.usage_metadata.job_id, ')')
        WHEN u.usage_metadata.cluster_id IS NOT NULL
            THEN CONCAT('interactive-', u.usage_metadata.cluster_id)
        WHEN u.usage_metadata.warehouse_id IS NOT NULL
            THEN CONCAT('sql-warehouse-', u.usage_metadata.warehouse_id)
        ELSE CONCAT('sku-', u.sku_name)
    END AS workload,
    u.sku_name,
    SUM(u.usage_quantity)                            AS total_dbus,
    SUM(u.usage_quantity * lp.pricing.default)       AS cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
    ON u.sku_name = lp.sku_name
   AND u.usage_date BETWEEN lp.price_start_time
                        AND COALESCE(lp.price_end_time, CURRENT_DATE())
WHERE u.usage_date >= DATE_SUB(CURRENT_DATE(), %(lookback)s)
GROUP BY u.usage_date, u.workspace_id, u.usage_metadata.job_id,
         u.usage_metadata.cluster_id, u.usage_metadata.warehouse_id,
         u.sku_name
"""


def _fetch_from_one_workspace(
    workspace: DatabricksWorkspace,
    lookback_days: int,
    ingested_at: datetime,
) -> list[dict]:
    """Run the usage × list_prices × jobs query against one workspace.

    Falls back to a no-jobs-table version if `system.lakeflow.jobs` isn't
    accessible (workload column will show "(unnamed job <id>)" instead of
    real names — every other column is unaffected).
    """
    rows: list[dict] = []
    with _connection(workspace) as con, con.cursor() as cur:
        try:
            cur.execute(_QUERY_WITH_JOB_NAMES, {"lookback": lookback_days})
        except Exception as e:
            msg = str(e).lower()
            if "lakeflow" in msg or "table_or_view_not_found" in msg or "permission" in msg:
                log.warning(
                    "[%s] system.lakeflow.jobs not accessible (%s) — "
                    "falling back to no-names query. Ask a metastore admin "
                    "to grant SELECT ON SCHEMA system.lakeflow to get names.",
                    workspace.name, type(e).__name__,
                )
                cur.execute(_QUERY_WITHOUT_JOB_NAMES, {"lookback": lookback_days})
            else:
                raise

        cols = [d[0] for d in cur.description]
        for raw in cur.fetchall():
            row = dict(zip(cols, raw))
            ud = row.get("usage_date")
            if isinstance(ud, datetime):
                row["usage_date"] = ud.date()
            row["total_dbus"] = float(row.get("total_dbus") or 0.0)
            row["cost_usd"] = float(row.get("cost_usd") or 0.0)
            row["ingested_at"] = ingested_at
            rows.append(row)
    return rows


def fetch_job_costs(
    lookback_days: int = DATABRICKS_LOOKBACK_DAYS,
) -> list[dict]:
    """Query every configured Databricks workspace and merge the results.

    If only one workspace is configured AND your metastore is shared, the
    single query will return rows for ALL workspaces (filtered by
    workspace_id). If your workspaces have separate metastores, configure
    each one in .env and this function will iterate over them.

    Per-workspace failures are logged but don't abort the whole pull —
    the dashboard section will just be missing those rows.
    """
    if not DATABRICKS_WORKSPACES:
        raise DatabricksAuthError(
            "No Databricks workspaces configured. Set either:\n"
            "  • DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN "
            "(legacy single-workspace), or\n"
            "  • DATABRICKS_<NAME>_HOST / _HTTP_PATH / _TOKEN for each\n"
            "    workspace you want to ingest (per-workspace pattern).\n"
            "See cost-monitor/.env.example."
        )

    ingested_at = datetime.utcnow()
    all_rows: list[dict] = []
    for ws in DATABRICKS_WORKSPACES:
        log.info("Querying Databricks workspace %s …", ws.name)
        try:
            rows = _fetch_from_one_workspace(ws, lookback_days, ingested_at)
            log.info("  [%s] %d rows", ws.name, len(rows))
            all_rows.extend(rows)
        except Exception as e:
            log.error("  [%s] failed: %s", ws.name, e)

    log.info("Fetched %d Databricks job-cost rows total", len(all_rows))
    return all_rows
