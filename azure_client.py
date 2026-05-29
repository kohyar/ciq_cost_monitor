"""Thin client around the Azure Cost Management Query + Forecast APIs.

Auth uses azure-identity's DefaultAzureCredential so it picks up either an
existing `az login` session or the AZURE_TENANT_ID / AZURE_CLIENT_ID /
AZURE_CLIENT_SECRET trio if they're set.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential

from config import (
    ADAPTIVE_DELAY_MAX_SECONDS,
    COST_API_BASE,
    COST_API_VERSION,
    INTER_REQUEST_DELAY_SECONDS,
    MANAGEMENT_SCOPE,
    MAX_RETRIES,
    RETRY_BASE_DELAY_SECONDS,
    RETRY_MAX_DELAY_SECONDS,
)

log = logging.getLogger(__name__)


class AuthError(RuntimeError):
    pass


class CostApiError(RuntimeError):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"Cost Management API returned {status}: {body[:500]}")
        self.status = status
        self.body = body


@dataclass
class QueryResult:
    columns: list[str]
    column_index: dict[str, int]
    rows: list[list[Any]]


class AzureCostClient:
    def __init__(self) -> None:
        self._credential = DefaultAzureCredential()
        self._token: str | None = None
        self._token_expires_at: float = 0.0
        self._last_request_at: float = 0.0
        # Starts at the conservative floor; ratchets up on each 429 within
        # this client instance's lifetime.
        self._adaptive_floor: float = INTER_REQUEST_DELAY_SECONDS

    # ---- auth ------------------------------------------------------------

    def _get_token(self) -> str:
        # Refresh ~60s before expiry so we don't fire a request with a stale token.
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token
        try:
            token = self._credential.get_token(MANAGEMENT_SCOPE)
        except ClientAuthenticationError as e:
            raise AuthError(
                "Failed to obtain an Azure access token. Run `az login` or set "
                "AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET."
            ) from e
        self._token = token.token
        self._token_expires_at = float(token.expires_on)
        return self._token

    def validate_access(self, subscription_id: str) -> None:
        """Issue a tiny daily query for yesterday to confirm Cost Mgmt access.

        Raises AuthError with a user-friendly message on 401/403.
        """
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        body = {
            "type": "AmortizedCost",
            "timeframe": "Custom",
            "timePeriod": {
                "from": _to_iso(yesterday),
                "to": _to_iso(yesterday),
            },
            "dataset": {
                "granularity": "Daily",
                "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
            },
        }
        try:
            self._post_query(subscription_id, body)
        except CostApiError as e:
            if e.status in (401, 403):
                raise AuthError(
                    f"Subscription {subscription_id} returned {e.status} from the "
                    "Cost Management API. Re-authenticate with `az login` or check "
                    "that the principal has Cost Management Reader on this scope."
                ) from e
            raise

    # ---- queries ---------------------------------------------------------

    def query_daily_by_resource(
        self,
        subscription_id: str,
        start: date,
        end: date,
    ) -> QueryResult:
        body = {
            "type": "AmortizedCost",
            "timeframe": "Custom",
            "timePeriod": {"from": _to_iso(start), "to": _to_iso(end)},
            "dataset": {
                "granularity": "Daily",
                "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
                "grouping": [
                    {"type": "Dimension", "name": "ServiceName"},
                    {"type": "Dimension", "name": "MeterSubCategory"},
                    {"type": "Dimension", "name": "ResourceId"},
                ],
            },
        }
        return self._run_paged_query(subscription_id, body)

    def query_monthly_by_service(
        self,
        subscription_id: str,
        start: date,
        end: date,
    ) -> QueryResult:
        """Monthly query, chunked into ≤364-day windows.

        Cost Management's Custom timeframe caps at ~1 year per request, so a
        2023→today pull has to be split. Results are merged into a single
        QueryResult.
        """
        merged_columns: list[str] = []
        merged_index: dict[str, int] = {}
        merged_rows: list[list[Any]] = []

        cursor = start
        while cursor <= end:
            chunk_end = min(cursor + timedelta(days=364), end)
            body = {
                "type": "AmortizedCost",
                "timeframe": "Custom",
                "timePeriod": {"from": _to_iso(cursor), "to": _to_iso(chunk_end)},
                "dataset": {
                    "granularity": "Monthly",
                    "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
                    "grouping": [
                        {"type": "Dimension", "name": "ServiceName"},
                    ],
                },
            }
            result = self._run_paged_query(subscription_id, body)
            if not merged_columns and result.columns:
                merged_columns = result.columns
                merged_index = result.column_index
            merged_rows.extend(result.rows)
            cursor = chunk_end + timedelta(days=1)

        return QueryResult(columns=merged_columns, column_index=merged_index, rows=merged_rows)

    def query_forecast_month_end(self, subscription_id: str) -> float | None:
        """Use the Cost Management forecast endpoint to project the current
        month's total. Returns None if the API has no forecast for this scope.
        """
        today = datetime.now(timezone.utc).date()
        month_start = today.replace(day=1)
        if month_start.month == 12:
            month_end = month_start.replace(year=month_start.year + 1, month=1) - timedelta(days=1)
        else:
            month_end = month_start.replace(month=month_start.month + 1) - timedelta(days=1)

        body = {
            "type": "AmortizedCost",
            "timeframe": "Custom",
            "timePeriod": {"from": _to_iso(month_start), "to": _to_iso(month_end)},
            "dataset": {
                "granularity": "None",
                "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
            },
            "includeActualCost": True,
            "includeFreshPartialCost": False,
        }
        url = (
            f"{COST_API_BASE}/subscriptions/{subscription_id}"
            f"/providers/Microsoft.CostManagement/forecast?api-version={COST_API_VERSION}"
        )
        try:
            payload = self._request_with_retries("POST", url, json=body)
        except CostApiError as e:
            # Forecast is best-effort. Some subscriptions return 400 when there
            # isn't enough history; treat that as "no projection".
            if e.status in (400, 404):
                log.warning("Forecast unavailable for %s (%s)", subscription_id, e.status)
                return None
            raise

        props = (payload or {}).get("properties") or {}
        rows = props.get("rows") or []
        columns = [c.get("name", "").lower() for c in props.get("columns") or []]
        if not rows or "cost" not in columns:
            return None
        cost_idx = columns.index("cost")
        return float(sum((row[cost_idx] or 0) for row in rows))

    # ---- internals -------------------------------------------------------

    def _post_query(self, subscription_id: str, body: dict) -> dict:
        url = (
            f"{COST_API_BASE}/subscriptions/{subscription_id}"
            f"/providers/Microsoft.CostManagement/query?api-version={COST_API_VERSION}"
        )
        return self._request_with_retries("POST", url, json=body)

    def _run_paged_query(self, subscription_id: str, body: dict) -> QueryResult:
        payload = self._post_query(subscription_id, body)
        columns: list[str] = []
        rows: list[list[Any]] = []
        while True:
            props = (payload or {}).get("properties") or {}
            col_names = [c.get("name", "") for c in props.get("columns") or []]
            if not columns:
                columns = col_names
            rows.extend(props.get("rows") or [])
            next_link = props.get("nextLink") or payload.get("nextLink")
            if not next_link:
                break
            # Cost Management's nextLink keeps the original POST method and body;
            # GET returns 405. The skiptoken in the URL tracks position.
            payload = self._request_with_retries("POST", next_link, json=body)
        column_index = {name.lower(): i for i, name in enumerate(columns)}
        return QueryResult(columns=columns, column_index=column_index, rows=rows)

    def _request_with_retries(
        self,
        method: str,
        url: str,
        *,
        json: dict | None = None,
    ) -> dict:
        # Adaptive floor between requests — ratchets up after each 429.
        delta = time.monotonic() - self._last_request_at
        if delta < self._adaptive_floor:
            time.sleep(self._adaptive_floor - delta)

        for attempt in range(MAX_RETRIES + 1):
            token = self._get_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            try:
                resp = requests.request(method, url, headers=headers, json=json, timeout=120)
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    raise CostApiError(0, f"network error: {e}") from e
                self._sleep_backoff(attempt)
                continue
            finally:
                self._last_request_at = time.monotonic()

            if resp.status_code < 400:
                if not resp.content:
                    return {}
                try:
                    return resp.json()
                except ValueError as e:
                    raise CostApiError(resp.status_code, f"invalid JSON: {e}")

            if resp.status_code == 429 or resp.status_code in (500, 502, 503, 504):
                # Raise the per-request floor for the rest of this run.
                if resp.status_code == 429:
                    prev = self._adaptive_floor
                    self._adaptive_floor = min(self._adaptive_floor * 1.5, ADAPTIVE_DELAY_MAX_SECONDS)
                    if self._adaptive_floor != prev:
                        log.info("Adaptive floor raised: %.1fs → %.1fs", prev, self._adaptive_floor)

                if attempt == MAX_RETRIES:
                    raise CostApiError(resp.status_code, resp.text)

                retry_after = _retry_after_seconds(resp)
                if retry_after is not None:
                    wait = min(retry_after, RETRY_MAX_DELAY_SECONDS)
                    log.info("Honoring Retry-After: sleeping %.1fs", wait)
                    time.sleep(wait)
                else:
                    self._sleep_backoff(attempt)
                continue

            raise CostApiError(resp.status_code, resp.text)

        # Shouldn't reach here.
        raise CostApiError(0, "exhausted retries without raising")

    def _sleep_backoff(self, attempt: int) -> None:
        delay = min(
            RETRY_BASE_DELAY_SECONDS * (2 ** attempt) + random.uniform(0, 1),
            RETRY_MAX_DELAY_SECONDS,
        )
        log.info("Backing off %.1fs (attempt %d)", delay, attempt + 1)
        time.sleep(delay)


def _to_iso(d: date) -> str:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).isoformat()


# Azure Cost Management uses several throttle headers; the standard
# Retry-After is often missing on 429. Check them all and take the longest.
_RETRY_AFTER_HEADERS: tuple[str, ...] = (
    "x-ms-ratelimit-microsoft.costmanagement-entity-retry-after",
    "x-ms-ratelimit-microsoft.costmanagement-tenant-retry-after",
    "x-ms-ratelimit-microsoft.consumption-retry-after",
    "x-ms-ratelimit-remaining-subscription-reads-retry-after",
    "Retry-After",
)


def _retry_after_seconds(resp: requests.Response) -> float | None:
    candidates: list[float] = []
    for name in _RETRY_AFTER_HEADERS:
        # requests headers are case-insensitive, but check both forms to be safe.
        raw = resp.headers.get(name) or resp.headers.get(name.lower())
        if not raw:
            continue
        try:
            candidates.append(float(raw))
        except ValueError:
            continue
    return max(candidates) if candidates else None
