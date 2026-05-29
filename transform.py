"""Pure functions that enrich raw Cost Management rows with derived columns.

Kept dependency-free (no duckdb / requests imports) so it is fast to unit test.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Optional

# Matches a resource id or name fragment that encodes its environment.
# Examples that should match:
#   /subscriptions/.../resourceGroups/rg-ciq-dev/...
#   k8s-ciq-staging
#   adb-ciq-production
#   saciqprod, saciqdev, saciqstaging
_ENV_FROM_NAME = re.compile(r"-ciq-(dev|staging|prod|production)\b", re.IGNORECASE)
_ENV_FROM_STORAGE = re.compile(r"\bsaciq(dev|staging|prod)\b", re.IGNORECASE)

# Subscription names follow ciq-<env>; this is the cheapest signal.
_ENV_FROM_SUB = re.compile(r"^ciq-(dev|staging|prod|production)$", re.IGNORECASE)


def _normalize_env(value: str | None) -> str | None:
    if not value:
        return None
    v = value.lower()
    if v == "production":
        return "prod"
    if v in {"dev", "staging", "prod"}:
        return v
    return None


def infer_environment(
    subscription_name: str | None,
    resource_id: str | None = None,
    resource_name: str | None = None,
    resource_group: str | None = None,
) -> str | None:
    """Subscription name first, then fall back to regex on resource fields."""
    if subscription_name:
        m = _ENV_FROM_SUB.match(subscription_name.strip())
        if m:
            return _normalize_env(m.group(1))

    haystack = " ".join(filter(None, [resource_id, resource_name, resource_group]))
    if not haystack:
        return None

    m = _ENV_FROM_NAME.search(haystack)
    if m:
        return _normalize_env(m.group(1))

    m = _ENV_FROM_STORAGE.search(haystack)
    if m:
        return _normalize_env(m.group(1))

    return None


# Service-family heuristics. Order matters: AKS detection must come BEFORE the
# generic "Virtual Machines" / storage rules because node VMs live in MC_*
# resource groups and would otherwise be misclassified.
_MC_RG = re.compile(r"^mc_", re.IGNORECASE)
_DATABRICKS_RG = re.compile(r"(^databricks-rg)|(\bdatabricks\b)", re.IGNORECASE)
_DATABRICKS_STORAGE = re.compile(r"^dbstorage", re.IGNORECASE)


def infer_service_family(
    service_name: str | None,
    resource_group: str | None = None,
    resource_id: str | None = None,
) -> str:
    rg = (resource_group or "").strip()
    svc = (service_name or "").strip().lower()
    rid = (resource_id or "").lower()

    # AKS: explicit service OR anything inside the managed MC_* RG.
    if "kubernetes" in svc or rg and _MC_RG.match(rg):
        return "aks"
    # If the resource id places it in an MC_ RG (even without the field) — same.
    if "/resourcegroups/mc_" in rid:
        return "aks"

    # Databricks: official service, the databricks-rg managed RG,
    # or dbstorage* storage accounts that the workspace provisions.
    if "databricks" in svc:
        return "databricks"
    if rg and _DATABRICKS_RG.search(rg):
        return "databricks"
    # Resource-id-based detection: the RG slug lives in the path even when
    # the API didn't echo back resource_group as its own field.
    if "/resourcegroups/databricks-rg" in rid or "databricks-rg" in rid.split("/")[-1:][0:1]:
        return "databricks"
    if _DATABRICKS_STORAGE.search(rg or ""):
        return "databricks"
    # dbstorage* storage account in the resource id (segment-anchored, not
    # substring, so we don't catch unrelated names that happen to contain it).
    segments = [s for s in rid.split("/") if s]
    if any(_DATABRICKS_STORAGE.search(seg) for seg in segments):
        return "databricks"

    if "storage" in svc:
        return "storage"
    if "key vault" in svc or "keyvault" in svc:
        return "keyvault"
    if any(token in svc for token in (
        "network", "load balancer", "bandwidth", "vpn", "application gateway",
        "public ip", "dns", "traffic manager", "front door",
    )):
        return "networking"

    return "other"


def is_databricks_managed(
    resource_group: str | None = None,
    resource_id: str | None = None,
) -> bool:
    rg = (resource_group or "")
    rid = (resource_id or "").lower()
    if _DATABRICKS_RG.search(rg):
        return True
    if _DATABRICKS_STORAGE.search(rg):
        return True
    if "/resourcegroups/databricks-rg" in rid:
        return True
    segments = [s for s in rid.split("/") if s]
    if any(_DATABRICKS_STORAGE.search(seg) for seg in segments):
        return True
    return False


def parse_usage_date(value) -> date | None:
    """Cost Management returns UsageDate as an int like 20260527."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    # int / "20260527"
    if len(s) == 8 and s.isdigit():
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    # ISO fallback
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def parse_billing_month(value) -> date | None:
    """BillingMonth comes back as ISO datetime; return the first-of-month date."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.replace(day=1)
    if isinstance(value, datetime):
        return value.date().replace(day=1)
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().replace(day=1)
    except ValueError:
        if len(s) == 8 and s.isdigit():
            return date(int(s[0:4]), int(s[4:6]), 1)
        return None


def extract_resource_group(resource_id: str | None) -> str | None:
    if not resource_id:
        return None
    m = re.search(r"/resourcegroups/([^/]+)", resource_id, re.IGNORECASE)
    return m.group(1) if m else None


def extract_resource_name(resource_id: str | None) -> str | None:
    if not resource_id:
        return None
    return resource_id.rstrip("/").split("/")[-1] or None


def build_daily_row(
    *,
    raw: dict,
    column_index: dict[str, int],
    subscription_id: str,
    environment: str,
    currency: str,
    ingested_at: datetime,
) -> Optional[dict]:
    """Build a normalized daily-cost row from a Cost Management API row.

    `column_index` maps a column name (lowercased) to its position in `raw`,
    since the API returns rows as positional arrays.

    `environment` is supplied by the caller — each Cost Management query is
    scoped to a specific subscription, and the Subscription record carries
    its own env label, so inferring it from resource names is unnecessary.
    """
    def col(name: str):
        idx = column_index.get(name.lower())
        if idx is None:
            return None
        return raw[idx] if idx < len(raw) else None

    cost = col("cost") if "cost" in column_index else col("pretaxcost")
    if cost is None:
        cost = col("costusd") or col("pretaxcostusd")
    if cost is None:
        return None
    try:
        cost_val = float(cost)
    except (TypeError, ValueError):
        return None

    usage_date = parse_usage_date(col("usagedate"))
    if usage_date is None:
        return None

    resource_id = col("resourceid")
    resource_group = col("resourcegroupname") or col("resourcegroup") or extract_resource_group(resource_id)
    resource_name = extract_resource_name(resource_id)
    service_name = col("servicename")
    meter_subcategory = col("metersubcategory")

    service_family = infer_service_family(service_name, resource_group, resource_id)

    row_currency = col("currency") or col("billingcurrencycode") or currency

    return {
        "date": usage_date,
        "subscription_id": subscription_id,
        "environment": environment,
        "service_family": service_family,
        "service_name": service_name,
        "meter_subcategory": meter_subcategory,
        "resource_group": resource_group,
        "resource_id": resource_id,
        "resource_name": resource_name,
        "cost": cost_val,
        "currency": row_currency,
        "ingested_at": ingested_at,
    }


def build_monthly_row(
    *,
    raw: dict,
    column_index: dict[str, int],
    subscription_id: str,
    environment: str,
    currency: str,
    ingested_at: datetime,
) -> Optional[dict]:
    def col(name: str):
        idx = column_index.get(name.lower())
        if idx is None:
            return None
        return raw[idx] if idx < len(raw) else None

    cost = col("cost") if "cost" in column_index else col("pretaxcost")
    if cost is None:
        cost = col("costusd") or col("pretaxcostusd")
    if cost is None:
        return None
    try:
        cost_val = float(cost)
    except (TypeError, ValueError):
        return None

    month_start = parse_billing_month(col("billingmonth")) or parse_usage_date(col("usagedate"))
    if month_start is None:
        return None
    month_start = month_start.replace(day=1)

    service_name = col("servicename")
    service_family = infer_service_family(service_name)
    row_currency = col("currency") or col("billingcurrencycode") or currency

    return {
        "month_start": month_start,
        "subscription_id": subscription_id,
        "environment": environment,
        "service_family": service_family,
        "service_name": service_name,
        "cost": cost_val,
        "currency": row_currency,
        "ingested_at": ingested_at,
    }
