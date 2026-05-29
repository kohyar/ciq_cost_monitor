"""Unit tests for the pure helpers in transform.py."""

from datetime import date, datetime

import pytest

from transform import (
    build_daily_row,
    extract_resource_group,
    extract_resource_name,
    infer_environment,
    infer_service_family,
    is_databricks_managed,
    parse_billing_month,
    parse_usage_date,
)


# ----- infer_environment ----------------------------------------------------

class TestInferEnvironment:
    def test_subscription_name_takes_priority(self):
        assert infer_environment("ciq-dev") == "dev"
        assert infer_environment("ciq-staging") == "staging"
        assert infer_environment("ciq-prod") == "prod"

    def test_production_normalizes_to_prod(self):
        assert infer_environment("ciq-production") == "prod"

    def test_resource_id_fallback_when_sub_unknown(self):
        rid = (
            "/subscriptions/x/resourceGroups/rg-ciq-staging/providers/"
            "Microsoft.ContainerService/managedClusters/k8s-ciq-staging"
        )
        assert infer_environment(None, resource_id=rid) == "staging"

    def test_storage_account_naming_fallback(self):
        assert infer_environment(None, resource_name="saciqprod") == "prod"
        assert infer_environment(None, resource_name="saciqstaging") == "staging"
        assert infer_environment(None, resource_name="saciqdev") == "dev"

    def test_kv_resource_name(self):
        assert infer_environment(None, resource_name="kv-ciq-production") == "prod"

    def test_returns_none_when_unparseable(self):
        assert infer_environment(None) is None
        assert infer_environment("unallocated_azure_plan_01") is None
        assert infer_environment(None, resource_name="random-thing") is None

    def test_sub_name_misses_dont_block_resource_fallback(self):
        # Subscription name doesn't encode env, but the resource clearly does.
        assert (
            infer_environment(
                "shared-platform",
                resource_id="/subscriptions/x/resourceGroups/rg-ciq-dev/providers/.../foo",
            )
            == "dev"
        )


# ----- infer_service_family -------------------------------------------------

class TestInferServiceFamily:
    def test_explicit_aks_service_name(self):
        assert infer_service_family("Azure Kubernetes Service") == "aks"

    def test_aks_node_vms_via_mc_resource_group(self):
        # Critical: AKS node VMs bill as "Virtual Machines" under MC_*. They
        # must be bucketed into aks or the total will be badly understated.
        assert (
            infer_service_family(
                "Virtual Machines",
                resource_group="MC_rg-ciq-prod_k8s-ciq-prod_eastus",
            )
            == "aks"
        )

    def test_aks_mc_resource_group_via_resource_id(self):
        rid = (
            "/subscriptions/x/resourceGroups/MC_rg-ciq-prod_k8s-ciq-prod_eastus/"
            "providers/Microsoft.Compute/virtualMachineScaleSets/aks-default-1234"
        )
        assert infer_service_family("Virtual Machines", resource_id=rid) == "aks"

    def test_aks_mc_lowercase(self):
        assert (
            infer_service_family(
                "Load Balancer",
                resource_group="mc_rg-ciq-staging_k8s-ciq-staging_eastus",
            )
            == "aks"
        )

    def test_databricks_service(self):
        assert infer_service_family("Azure Databricks") == "databricks"

    def test_databricks_managed_rg(self):
        assert (
            infer_service_family(
                "Virtual Machines",
                resource_group="databricks-rg-adb-ciq-prod-abc123",
            )
            == "databricks"
        )

    def test_databricks_dbstorage_account(self):
        assert (
            infer_service_family(
                "Storage",
                resource_group="dbstorageabcdef",
            )
            == "databricks"
        )
        rid = (
            "/subscriptions/x/resourceGroups/databricks-rg-foo/providers/"
            "Microsoft.Storage/storageAccounts/dbstoragexyz"
        )
        assert infer_service_family("Storage", resource_id=rid) == "databricks"

    def test_storage_family(self):
        assert infer_service_family("Storage") == "storage"

    def test_keyvault_family(self):
        assert infer_service_family("Key Vault") == "keyvault"

    def test_networking_family(self):
        for svc in [
            "Bandwidth",
            "Virtual Network",
            "Load Balancer",
            "Application Gateway",
            "Azure DNS",
        ]:
            assert infer_service_family(svc) == "networking", svc

    def test_other_default(self):
        assert infer_service_family("Cognitive Services") == "other"
        assert infer_service_family(None) == "other"


# ----- is_databricks_managed ------------------------------------------------

class TestIsDatabricksManaged:
    def test_databricks_rg(self):
        assert is_databricks_managed(resource_group="databricks-rg-abc")

    def test_dbstorage(self):
        assert is_databricks_managed(resource_group="dbstoragefoo")
        assert is_databricks_managed(
            resource_id="/subscriptions/x/resourceGroups/databricks-rg-foo/providers/"
            "Microsoft.Storage/storageAccounts/dbstoragexyz"
        )

    def test_negative(self):
        assert not is_databricks_managed(resource_group="rg-ciq-dev")
        assert not is_databricks_managed(resource_id="/subscriptions/x/resourceGroups/rg-ciq-dev/providers/foo")


# ----- date parsing ---------------------------------------------------------

class TestDateParsing:
    def test_usage_date_int(self):
        assert parse_usage_date(20260527) == date(2026, 5, 27)

    def test_usage_date_string(self):
        assert parse_usage_date("20260101") == date(2026, 1, 1)

    def test_usage_date_iso(self):
        assert parse_usage_date("2026-05-27T00:00:00Z") == date(2026, 5, 27)

    def test_usage_date_none(self):
        assert parse_usage_date(None) is None
        assert parse_usage_date("") is None

    def test_billing_month_iso(self):
        assert parse_billing_month("2026-05-01T00:00:00+00:00") == date(2026, 5, 1)

    def test_billing_month_int(self):
        assert parse_billing_month(20260501) == date(2026, 5, 1)


# ----- resource id helpers --------------------------------------------------

class TestResourceIdHelpers:
    def test_extract_resource_group(self):
        rid = (
            "/subscriptions/abc/resourceGroups/rg-ciq-dev/providers/"
            "Microsoft.ContainerService/managedClusters/k8s-ciq-dev"
        )
        assert extract_resource_group(rid) == "rg-ciq-dev"

    def test_extract_resource_group_case_insensitive(self):
        rid = "/subscriptions/abc/resourcegroups/rg-ciq-dev/providers/foo"
        assert extract_resource_group(rid) == "rg-ciq-dev"

    def test_extract_resource_name(self):
        rid = (
            "/subscriptions/abc/resourceGroups/rg-ciq-dev/providers/"
            "Microsoft.ContainerService/managedClusters/k8s-ciq-dev"
        )
        assert extract_resource_name(rid) == "k8s-ciq-dev"


# ----- build_daily_row -----------------------------------------------------

class TestBuildDailyRow:
    @pytest.fixture
    def column_index(self):
        # Mirrors what Cost Management returns when grouping by service / meter
        # / resource id.
        cols = [
            "Cost",
            "UsageDate",
            "ServiceName",
            "MeterSubCategory",
            "ResourceId",
            "Currency",
        ]
        return {c.lower(): i for i, c in enumerate(cols)}

    def test_happy_path(self, column_index):
        ingested_at = datetime(2026, 5, 27, 12, 0, 0)
        row = build_daily_row(
            raw=[
                12.34,
                20260527,
                "Azure Kubernetes Service",
                "Standard",
                "/subscriptions/abc/resourceGroups/rg-ciq-dev/providers/"
                "Microsoft.ContainerService/managedClusters/k8s-ciq-dev",
                "USD",
            ],
            column_index=column_index,
            subscription_id="abc",
            environment="dev",
            currency="USD",
            ingested_at=ingested_at,
        )
        assert row["date"] == date(2026, 5, 27)
        assert row["environment"] == "dev"
        assert row["service_family"] == "aks"
        assert row["resource_group"] == "rg-ciq-dev"
        assert row["resource_name"] == "k8s-ciq-dev"
        assert row["cost"] == pytest.approx(12.34)
        assert row["currency"] == "USD"
        assert row["ingested_at"] == ingested_at

    def test_aks_node_vm_in_mc_rg(self, column_index):
        """A 'Virtual Machines' row inside MC_* must bucket to aks."""
        ingested_at = datetime(2026, 5, 27, 12, 0, 0)
        row = build_daily_row(
            raw=[
                88.0,
                20260527,
                "Virtual Machines",
                "Dv3/DSv3 Series",
                "/subscriptions/abc/resourceGroups/MC_rg-ciq-prod_k8s-ciq-prod_eastus/"
                "providers/Microsoft.Compute/virtualMachineScaleSets/aks-pool-1",
                "USD",
            ],
            column_index=column_index,
            subscription_id="abc",
            environment="prod",
            currency="USD",
            ingested_at=ingested_at,
        )
        assert row["service_family"] == "aks"
        assert row["environment"] == "prod"

    def test_non_ciq_env_passes_through(self, column_index):
        """Non-CIQ subs (e.g. forecaster-dev) should use the caller-supplied env."""
        row = build_daily_row(
            raw=[
                5.0,
                20260527,
                "Virtual Machines",
                None,
                "/subscriptions/xyz/resourceGroups/rg-forecaster-dev/providers/"
                "Microsoft.Compute/virtualMachines/forecaster-vm-1",
                "USD",
            ],
            column_index=column_index,
            subscription_id="xyz",
            environment="forecaster-dev",
            currency="USD",
            ingested_at=datetime(2026, 5, 27, 12, 0, 0),
        )
        assert row["environment"] == "forecaster-dev"

    def test_skips_row_without_cost(self, column_index):
        row = build_daily_row(
            raw=[None, 20260527, "Storage", None, None, "USD"],
            column_index=column_index,
            subscription_id="abc",
            environment="dev",
            currency="USD",
            ingested_at=datetime.now(),
        )
        assert row is None
