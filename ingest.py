"""Ingestion entrypoint.

Pulls a fixed window every run:
  * Daily AmortizedCost, last 90 days (DAILY_LOOKBACK_DAYS in config.py)
  * Monthly AmortizedCost from 2023-01-01 → today (MONTHLY_START_DATE)
  * Forecast for the current calendar month

Usage:
    python ingest.py                 # all three subscriptions
    python ingest.py --only dev      # one environment
    python ingest.py --skip-forecast # skip the forecast call
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

from azure_client import AuthError, AzureCostClient, CostApiError
from config import (
    DAILY_LOOKBACK_DAYS,
    EXPECTED_CURRENCY,
    MONTHLY_START_DATE,
    SUBSCRIPTIONS,
    Subscription,
)
from db import (
    connect,
    init_schema,
    record_run,
    replace_forecast,
    upload_db_to_blob,
    upsert_daily,
    upsert_monthly,
)
from transform import build_daily_row, build_monthly_row


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest Azure cost data into local DuckDB.")
    p.add_argument(
        "--skip-forecast",
        action="store_true",
        help="Skip the month-end forecast call.",
    )
    p.add_argument(
        "--only",
        choices=[s.environment for s in SUBSCRIPTIONS],
        help="Limit to a single environment (e.g. --only dev).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def _resolve_subscriptions(only: str | None) -> list[Subscription]:
    if only:
        return [s for s in SUBSCRIPTIONS if s.environment == only]
    return list(SUBSCRIPTIONS)


def _ingest_subscription(
    client: AzureCostClient,
    sub: Subscription,
    do_forecast: bool,
    con,
) -> dict:
    log = logging.getLogger(f"ingest.{sub.environment}")
    today = datetime.now(timezone.utc).date()
    daily_start = today - timedelta(days=DAILY_LOOKBACK_DAYS - 1)
    daily_end = today

    monthly_start = MONTHLY_START_DATE
    monthly_end = today

    summary = {"subscription": sub.name, "daily_rows": 0, "monthly_rows": 0, "forecast": None}

    # ----- daily ---------------------------------------------------------
    started = datetime.now(timezone.utc)
    try:
        log.info("Daily query %s → %s", daily_start, daily_end)
        result = client.query_daily_by_resource(sub.id, daily_start, daily_end)
        ingested_at = datetime.now(timezone.utc)
        rows = []
        for raw in result.rows:
            row = build_daily_row(
                raw=raw,
                column_index=result.column_index,
                subscription_id=sub.id,
                environment=sub.environment,
                currency=EXPECTED_CURRENCY,
                ingested_at=ingested_at,
            )
            if row is not None:
                rows.append(row)
        written = upsert_daily(con, rows)
        summary["daily_rows"] = written
        record_run(con, sub.id, "daily", started, datetime.now(timezone.utc), written, "success", None)
        log.info("Daily: wrote %d rows", written)
    except (AuthError, CostApiError) as e:
        record_run(con, sub.id, "daily", started, datetime.now(timezone.utc), None, "error", str(e))
        raise

    # ----- monthly -------------------------------------------------------
    started = datetime.now(timezone.utc)
    try:
        log.info("Monthly query %s → %s", monthly_start, monthly_end)
        result = client.query_monthly_by_service(sub.id, monthly_start, monthly_end)
        ingested_at = datetime.now(timezone.utc)
        rows = []
        for raw in result.rows:
            row = build_monthly_row(
                raw=raw,
                column_index=result.column_index,
                subscription_id=sub.id,
                environment=sub.environment,
                currency=EXPECTED_CURRENCY,
                ingested_at=ingested_at,
            )
            if row is not None:
                rows.append(row)
        written = upsert_monthly(con, rows)
        summary["monthly_rows"] = written
        record_run(con, sub.id, "monthly", started, datetime.now(timezone.utc), written, "success", None)
        log.info("Monthly: wrote %d rows", written)
    except (AuthError, CostApiError) as e:
        record_run(con, sub.id, "monthly", started, datetime.now(timezone.utc), None, "error", str(e))
        raise

    # ----- forecast ------------------------------------------------------
    if do_forecast:
        started = datetime.now(timezone.utc)
        try:
            log.info("Forecast query for current month")
            projected = client.query_forecast_month_end(sub.id)
            summary["forecast"] = projected
            if projected is not None:
                replace_forecast(
                    con,
                    [{
                        "subscription_id": sub.id,
                        "environment": sub.environment,
                        "projected_month_end": projected,
                        "as_of": datetime.now(timezone.utc).date(),
                    }],
                )
            record_run(con, sub.id, "forecast", started, datetime.now(timezone.utc), 1 if projected else 0, "success", None)
        except (AuthError, CostApiError) as e:
            record_run(con, sub.id, "forecast", started, datetime.now(timezone.utc), None, "error", str(e))
            log.warning("Forecast failed for %s: %s", sub.name, e)

    return summary


def main() -> int:
    args = _parse_args()
    _setup_logging(args.verbose)
    log = logging.getLogger("ingest")

    subscriptions = _resolve_subscriptions(args.only)
    if not subscriptions:
        log.error("No subscriptions selected.")
        return 2

    client = AzureCostClient()

    # Validate against the first subscription before doing the full pull.
    try:
        log.info("Validating access against %s …", subscriptions[0].name)
        client.validate_access(subscriptions[0].id)
    except AuthError as e:
        print(f"\nAuth error: {e}\n", file=sys.stderr)
        return 3

    try:
        con = connect()
    except Exception as e:
        if "lock" in str(e).lower():
            print(
                "\nCould not open costs.duckdb — another process holds the lock.\n"
                "Stop the Streamlit dashboard (Ctrl+C in that terminal) and re-run.\n",
                file=sys.stderr,
            )
            return 4
        raise
    try:
        init_schema(con)
        summaries = []
        for sub in subscriptions:
            try:
                summaries.append(
                    _ingest_subscription(
                        client=client,
                        sub=sub,
                        do_forecast=not args.skip_forecast,
                        con=con,
                    )
                )
            except (AuthError, CostApiError) as e:
                log.error("Failed for %s: %s", sub.name, e)
                summaries.append({"subscription": sub.name, "error": str(e)})
    finally:
        con.close()

    print("\nIngest summary:")
    any_success = False
    for s in summaries:
        if "error" in s:
            print(f"  {s['subscription']:14s} ERROR  {s['error'][:120]}")
        else:
            any_success = True
            forecast = f"projected ${s['forecast']:,.0f}" if s.get("forecast") is not None else "no forecast"
            print(
                f"  {s['subscription']:14s} daily={s['daily_rows']:>5d}  "
                f"monthly={s['monthly_rows']:>3d}  {forecast}"
            )

    # Mirror the fresh DuckDB to Azure Blob so a hosted dashboard
    # (e.g. Streamlit Cloud) sees the new data. No-op if AZURE_BLOB_SAS_URL
    # isn't configured.
    if any_success:
        try:
            if upload_db_to_blob():
                print("Uploaded costs.duckdb to Azure Blob.")
        except Exception as e:
            log.error("Failed to upload to blob: %s", e)
            print(f"\nWARNING: blob upload failed: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
