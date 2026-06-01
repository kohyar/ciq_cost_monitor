"""Streamlit dashboard. Reads from DuckDB only — never calls the Azure API.

Run with:
    streamlit run dashboard.py
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

# Promote Streamlit Cloud secrets into env vars BEFORE importing config,
# so config.py can resolve required vars (subscription IDs, blob URL,
# etc.) via os.getenv regardless of whether we're running locally with
# .env or on Streamlit Cloud with secrets.toml.
try:
    for _key, _val in dict(st.secrets).items():
        if isinstance(_val, str) and _key not in os.environ:
            os.environ[_key] = _val
except (FileNotFoundError, AttributeError):
    pass  # secrets.toml not present — fine for local dev

from config import (  # noqa: E402
    AZURE_BLOB_SAS_URL,
    DATABRICKS_EXCLUDED_SKUS,
    DATABRICKS_LOOKBACK_DAYS,
    DATABRICKS_WORKSPACE_TO_ENV,
    DB_PATH,
    databricks_configured,
)
from db import ensure_local_db  # noqa: E402

st.set_page_config(page_title="CIQ Azure Cost Monitor", layout="wide")


# ---- Auth gate ------------------------------------------------------------
# If APP_USERNAME + APP_PASSWORD are set (via .env locally or Streamlit Cloud
# Secrets), every visitor must sign in before they see any cost data. Both
# unset → app is open (useful for local dev / debugging without typing the
# password every restart).
import secrets as _stdlib_secrets  # noqa: E402

_APP_USER = os.getenv("APP_USERNAME", "").strip()
_APP_PASS = os.getenv("APP_PASSWORD", "").strip()


def _require_login() -> None:
    if not _APP_USER or not _APP_PASS:
        return
    if st.session_state.get("authenticated"):
        return

    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.title("CIQ Azure Cost Monitor")
        st.caption("Sign in to continue.")
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("Username", autocomplete="username")
            password = st.text_input("Password", type="password", autocomplete="current-password")
            submitted = st.form_submit_button("Sign in", type="primary")
        if submitted:
            # Constant-time compare to avoid trivial timing oracles.
            ok_user = _stdlib_secrets.compare_digest(username, _APP_USER)
            ok_pass = _stdlib_secrets.compare_digest(password, _APP_PASS)
            if ok_user and ok_pass:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Invalid username or password.")
    st.stop()


_require_login()

CIQ_ENVS = ["ciq-dev", "ciq-staging", "ciq-prod"]
FORECASTER_ENVS = ["forecaster-dev"]
ENV_ORDER = CIQ_ENVS + FORECASTER_ENVS
ENV_COLORS = {
    "ciq-dev": "#3b82f6",
    "ciq-staging": "#f59e0b",
    "ciq-prod": "#ef4444",
    "forecaster-dev": "#8b5cf6",
}


# ----- data access ---------------------------------------------------------
#
# Connections are opened per query and closed immediately. A long-lived
# connection (even read-only) blocks the ingester from opening a writer, so
# we keep the lock window as short as possible.

@contextmanager
def _read_conn(db_path: str):
    con = duckdb.connect(db_path, read_only=True)
    try:
        yield con
    finally:
        con.close()


@st.cache_resource(show_spinner="Downloading cost data…")
def _bootstrap_db() -> bool:
    """Make sure costs.duckdb is available.

    Local path:  file already exists → return True.
    Cloud path:  AZURE_BLOB_SAS_URL is set → download from blob, cache the
                 result for the session (cleared on every app restart).
    """
    try:
        ensure_local_db()
        return True
    except FileNotFoundError as e:
        st.error(str(e))
        return False
    except Exception as e:
        st.error(
            f"Failed to fetch cost data: {e}\n\n"
            "Check that AZURE_BLOB_SAS_URL is valid and the SAS token hasn't expired."
        )
        return False


def _ensure_db_exists() -> bool:
    return _bootstrap_db()


@st.cache_data(ttl=300, show_spinner=False)
def load_daily(db_path: str) -> pd.DataFrame:
    with _read_conn(db_path) as con:
        return con.execute(
            """
            SELECT date, subscription_id, environment, service_family, service_name,
                   meter_subcategory, resource_group, resource_id, resource_name,
                   cost, currency
            FROM azure_costs
            """
        ).df()


@st.cache_data(ttl=300, show_spinner=False)
def load_monthly(db_path: str) -> pd.DataFrame:
    with _read_conn(db_path) as con:
        return con.execute(
            """
            SELECT month_start, subscription_id, environment, service_family,
                   service_name, cost, currency
            FROM azure_costs_monthly
            """
        ).df()


@st.cache_data(ttl=300, show_spinner=False)
def load_forecast(db_path: str) -> pd.DataFrame:
    with _read_conn(db_path) as con:
        return con.execute(
            """
            SELECT subscription_id, environment, projected_month_end, as_of
            FROM azure_forecast
            """
        ).df()


@st.cache_data(ttl=300, show_spinner=False)
def last_ingest(db_path: str):
    with _read_conn(db_path) as con:
        row = con.execute(
            "SELECT MAX(finished_at) FROM ingest_runs WHERE status = 'success'"
        ).fetchone()
        return row[0] if row else None


@st.cache_data(ttl=600, show_spinner="Loading Databricks job costs…")
def load_databricks_jobs() -> pd.DataFrame:
    """Live fetch from Databricks SQL warehouse (NOT from DuckDB).

    Returns an empty DataFrame when Databricks isn't configured, the
    warehouse is unreachable, or the user lacks permissions — the
    dashboard section just disappears in that case. Cached for 10 min
    so a wave of viewers doesn't spam the warehouse.
    """
    from config import databricks_configured
    if not databricks_configured():
        return pd.DataFrame()
    try:
        from databricks_client import fetch_job_costs
        rows = fetch_job_costs()
    except Exception as exc:
        # Surface as a warning but don't break the rest of the dashboard.
        st.warning(f"Databricks job-cost fetch failed: {exc}")
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ----- helpers -------------------------------------------------------------

def _fmt_usd(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"${value:,.2f}"


def _mtd_window(today: date) -> tuple[date, date]:
    return today.replace(day=1), today


def _prev_mtd_window(today: date) -> tuple[date, date]:
    first_of_this_month = today.replace(day=1)
    last_of_prev = first_of_this_month - timedelta(days=1)
    first_of_prev = last_of_prev.replace(day=1)
    # Align day count to the equivalent day-of-month window (so a partial month
    # comparison stays apples-to-apples).
    end_day = min(today.day, last_of_prev.day)
    return first_of_prev, first_of_prev.replace(day=end_day)


# ----- app -----------------------------------------------------------------

if not _ensure_db_exists():
    st.stop()

daily = load_daily(str(DB_PATH))
monthly = load_monthly(str(DB_PATH))
forecast = load_forecast(str(DB_PATH))
last_run = last_ingest(str(DB_PATH))
# dbx_jobs is fetched lazily inside its section when the user clicks Load.

if daily.empty:
    st.warning("No data in `azure_costs` yet. Run `python ingest.py --backfill 90`.")
    st.stop()

daily["date"] = pd.to_datetime(daily["date"]).dt.date
monthly["month_start"] = pd.to_datetime(monthly["month_start"]).dt.date

today = max(daily["date"].max(), date.today() - timedelta(days=1))
data_min = daily["date"].min()

# ---- header ---------------------------------------------------------------

st.title("CIQ Azure Cost Monitor")
caption_bits = []
if last_run is not None:
    caption_bits.append(f"Last ingested at **{pd.to_datetime(last_run):%Y-%m-%d %H:%M UTC}**")
caption_bits.append(
    "Azure Cost Management data lags 8–24h; figures are **not** real-time. Currency: USD."
)
st.caption(" · ".join(caption_bits))

# ---- sidebar --------------------------------------------------------------

with st.sidebar:
    st.header("Filters")
    env_choice = st.selectbox(
        "Environment",
        options=["all"] + [e for e in ENV_ORDER if e in set(daily["environment"].dropna())],
        index=0,
    )
    default_start = max(data_min, today - timedelta(days=29))
    date_range = st.date_input(
        "Date range",
        value=(default_start, today),
        min_value=data_min,
        max_value=today,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_dt, end_dt = date_range
    else:
        start_dt, end_dt = default_start, today

    if _APP_USER:
        st.divider()
        if st.button("Sign out"):
            st.session_state.authenticated = False
            st.rerun()


def _filter_env(df: pd.DataFrame, col: str = "environment") -> pd.DataFrame:
    if env_choice == "all":
        return df
    return df[df[col] == env_choice]


filtered_daily = _filter_env(daily)
filtered_daily = filtered_daily[
    (filtered_daily["date"] >= start_dt) & (filtered_daily["date"] <= end_dt)
]

# ---- Summary table --------------------------------------------------------

mtd_start, mtd_end = _mtd_window(today)
prev_start, prev_end = _prev_mtd_window(today)
current_year = today.year
last_year = today.year - 1
# Current year is partial. Exclude the in-progress current month entirely:
# the Total covers Jan → end of last completed month, and the average
# divides by the same number of months. (Including May in the Total but
# dividing by 4 was inconsistent.)
current_year_completed_months = max(today.month - 1, 1)
current_month_start = today.replace(day=1)

mtd_by_env = (
    daily[(daily["date"] >= mtd_start) & (daily["date"] <= mtd_end)]
    .groupby("environment", dropna=False)["cost"]
    .sum()
)
prev_by_env = (
    daily[(daily["date"] >= prev_start) & (daily["date"] <= prev_end)]
    .groupby("environment", dropna=False)["cost"]
    .sum()
)
monthly_year_series = pd.to_datetime(monthly["month_start"]).dt.year
last_year_by_env = (
    monthly[monthly_year_series == last_year]
    .groupby("environment", dropna=False)["cost"]
    .sum()
)
this_year_by_env = (
    monthly[
        (monthly_year_series == current_year)
        & (monthly["month_start"] != current_month_start)
    ]
    .groupby("environment", dropna=False)["cost"]
    .sum()
)
forecast_by_env = (
    forecast.set_index("environment")["projected_month_end"]
    if not forecast.empty
    else pd.Series(dtype=float)
)


def _delta_str(curr: float, prev: float) -> str:
    if not prev:
        return "—"
    pct = (curr - prev) / prev * 100
    return f"{pct:+.1f}%"


def _row_for_env(label: str, mtd: float, prev: float, ly: float, cy: float, proj) -> dict:
    return {
        "Environment": label,
        "MTD": _fmt_usd(mtd),
        "vs prior MTD": _delta_str(mtd, prev),
        "Projected month-end": (
            _fmt_usd(float(proj)) if proj is not None and not pd.isna(proj) else "—"
        ),
        f"Total {last_year}": _fmt_usd(ly),
        f"Avg / month {last_year}": _fmt_usd(ly / 12),
        f"Total {current_year} (closed mo.)": _fmt_usd(cy),
        f"Avg / month {current_year}": _fmt_usd(cy / current_year_completed_months),
    }


def _build_summary_table(envs: list[str], total_label: str | None) -> pd.DataFrame:
    rows = []
    for env in envs:
        rows.append(_row_for_env(
            env,
            float(mtd_by_env.get(env, 0.0)),
            float(prev_by_env.get(env, 0.0)),
            float(last_year_by_env.get(env, 0.0)),
            float(this_year_by_env.get(env, 0.0)),
            forecast_by_env.get(env),
        ))
    if total_label is not None and len(envs) > 1:
        t_mtd = sum(float(mtd_by_env.get(e, 0.0)) for e in envs)
        t_prev = sum(float(prev_by_env.get(e, 0.0)) for e in envs)
        t_ly = sum(float(last_year_by_env.get(e, 0.0)) for e in envs)
        t_cy = sum(float(this_year_by_env.get(e, 0.0)) for e in envs)
        proj_vals = [
            float(forecast_by_env.get(e))
            for e in envs
            if e in forecast_by_env.index and not pd.isna(forecast_by_env.get(e))
        ]
        t_proj = sum(proj_vals) if proj_vals else None
        rows.append(_row_for_env(total_label, t_mtd, t_prev, t_ly, t_cy, t_proj))
    return pd.DataFrame(rows)


st.subheader("Summary")

st.markdown("**CIQ**")
ciq_df = _build_summary_table(CIQ_ENVS, total_label="Total CIQ")
st.dataframe(ciq_df, hide_index=True, use_container_width=True)

st.markdown("**Forecaster**")
fc_df = _build_summary_table(FORECASTER_ENVS, total_label="Total Forecaster")
st.dataframe(fc_df, hide_index=True, use_container_width=True)

st.markdown("**Combined**")
grand_mtd = sum(float(mtd_by_env.get(e, 0.0)) for e in ENV_ORDER)
grand_prev = sum(float(prev_by_env.get(e, 0.0)) for e in ENV_ORDER)
grand_ly = sum(float(last_year_by_env.get(e, 0.0)) for e in ENV_ORDER)
grand_cy = sum(float(this_year_by_env.get(e, 0.0)) for e in ENV_ORDER)
grand_proj_vals = [
    float(forecast_by_env.get(e))
    for e in ENV_ORDER
    if e in forecast_by_env.index and not pd.isna(forecast_by_env.get(e))
]
grand_proj = sum(grand_proj_vals) if grand_proj_vals else None
grand_df = pd.DataFrame([
    _row_for_env("CIQ + Forecaster", grand_mtd, grand_prev, grand_ly, grand_cy, grand_proj)
])
st.dataframe(grand_df, hide_index=True, use_container_width=True)

st.caption(
    f"MTD = {mtd_start} → {mtd_end}. Prior-MTD window for delta = {prev_start} → {prev_end}. "
    f"Total {last_year} covers the full calendar year (avg ÷ 12). "
    f"Total {current_year} covers only closed months "
    f"(Jan → {(current_month_start - timedelta(days=1)).strftime('%b')}); "
    f"avg ÷ {current_year_completed_months}. The in-progress current month is excluded entirely "
    "so the Total and Avg are computed over the same set of months — look at the MTD column "
    "for the in-progress month's spend so far."
)

st.divider()

# ---- Daily trend, stacked by service_family -------------------------------

st.subheader(f"Daily cost — last {(end_dt - start_dt).days + 1} days")
daily_chart_df = (
    filtered_daily.groupby(["date", "service_family"], dropna=False)["cost"]
    .sum()
    .reset_index()
)
if daily_chart_df.empty:
    st.info("No rows in the selected window.")
else:
    daily_totals = daily_chart_df.groupby("date", as_index=False)["cost"].sum()
    bars = (
        alt.Chart(daily_chart_df)
        .mark_bar()
        .encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("cost:Q", title="Cost (USD)", stack="zero"),
            color=alt.Color("service_family:N", title="Service family"),
            tooltip=[
                alt.Tooltip("date:T"),
                "service_family:N",
                alt.Tooltip("cost:Q", format="$,.2f"),
            ],
        )
    )
    labels = (
        alt.Chart(daily_totals)
        .mark_text(dy=-6, fontSize=9, color="#1f2937")
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("cost:Q"),
            text=alt.Text("cost:Q", format="$,.0f"),
            tooltip=[alt.Tooltip("date:T"), alt.Tooltip("cost:Q", format="$,.2f")],
        )
    )
    st.altair_chart((bars + labels).properties(height=340), use_container_width=True)

# ---- Monthly trend — per env + total -------------------------------------

st.subheader("Monthly cost — Jan 2023 onward")
st.caption(
    "Each environment shown separately, then combined. Bars are stacked by "
    "service family. Sidebar env filter does not apply to this section."
)

def _monthly_chart(df: pd.DataFrame, title: str) -> alt.Chart:
    df = df.fillna({"service_family": "other"})
    totals = df.groupby("month_start", as_index=False)["cost"].sum()
    bars = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("yearmonth(month_start):T", title=None),
            y=alt.Y("cost:Q", title="Cost (USD)", stack="zero"),
            color=alt.Color("service_family:N", title="Service family"),
            tooltip=[
                alt.Tooltip("yearmonth(month_start):T", title="month"),
                "service_family:N",
                alt.Tooltip("cost:Q", format="$,.2f"),
            ],
        )
    )
    labels = (
        alt.Chart(totals)
        .mark_text(dy=-7, fontSize=10, color="#1f2937")
        .encode(
            x=alt.X("yearmonth(month_start):T"),
            y=alt.Y("cost:Q"),
            text=alt.Text("cost:Q", format="$,.0f"),
            tooltip=[
                alt.Tooltip("yearmonth(month_start):T", title="month"),
                alt.Tooltip("cost:Q", format="$,.2f"),
            ],
        )
    )
    return (bars + labels).properties(height=280, title=title)


if monthly.empty:
    st.info("No monthly data yet — run `python ingest.py`.")
else:
    for env in ENV_ORDER:
        env_df = monthly[monthly["environment"] == env]
        env_chart_df = (
            env_df.groupby(["month_start", "service_family"], dropna=False)["cost"]
            .sum()
            .reset_index()
        )
        if env_chart_df.empty:
            st.info(f"No monthly data for **{env}**.")
            continue
        st.altair_chart(
            _monthly_chart(env_chart_df, f"{env}"),
            use_container_width=True,
        )

    total_chart_df = (
        monthly.groupby(["month_start", "service_family"], dropna=False)["cost"]
        .sum()
        .reset_index()
    )
    st.altair_chart(
        _monthly_chart(total_chart_df, "All environments (total)"),
        use_container_width=True,
    )

# ---- Top 10 resources -----------------------------------------------------

st.subheader(f"Resources by spend ({env_choice})")
all_resources = (
    filtered_daily[filtered_daily["resource_id"].notna() & (filtered_daily["resource_id"] != "")]
    .groupby(["resource_name", "resource_group", "service_family", "environment"], dropna=False)["cost"]
    .sum()
    .reset_index()
    .sort_values("cost", ascending=False)
)
if all_resources.empty:
    st.info("No resource-attributed costs in this window.")
else:
    st.caption(f"{len(all_resources):,} resources in the selected window, sorted by total cost (descending).")
    all_resources["cost"] = all_resources["cost"].map(lambda v: f"${v:,.2f}")
    st.dataframe(all_resources, hide_index=True, use_container_width=True)

# ---- Databricks breakdown -------------------------------------------------

st.subheader("Databricks breakdown (MeterSubCategory)")
dbx = filtered_daily[filtered_daily["service_family"] == "databricks"]
dbx_breakdown = (
    dbx.groupby("meter_subcategory", dropna=False)["cost"].sum().reset_index()
    .sort_values("cost", ascending=False)
)
if dbx_breakdown.empty:
    st.info("No Databricks spend in the selected window.")
else:
    chart = (
        alt.Chart(dbx_breakdown.fillna({"meter_subcategory": "(unspecified)"}))
        .mark_bar()
        .encode(
            x=alt.X("cost:Q", title="Cost (USD)"),
            y=alt.Y("meter_subcategory:N", sort="-x", title=None),
            tooltip=["meter_subcategory:N", alt.Tooltip("cost:Q", format="$,.2f")],
        )
        .properties(height=max(120, 28 * len(dbx_breakdown)))
    )
    st.altair_chart(chart, use_container_width=True)

# ---- Databricks jobs (informational drill-down) ---------------------------
# These costs are ALREADY included in the Azure rollup under the "databricks"
# service family — never sum them into totals. This section is just per-job
# attribution for the spend you already see above.

if databricks_configured():
    st.subheader("Databricks jobs — per-workload spend (informational)")
    st.caption(
        "Already counted in Azure totals under the `databricks` service "
        "family. Shown separately so you can see which jobs / clusters "
        "drive the spend. Source: `system.billing.usage` × "
        "`system.billing.list_prices` × `system.lakeflow.jobs`. "
        "Data is fetched live from the SQL warehouse on demand."
    )

    # ---- Section-local filters (independent of the sidebar) --------------
    # Environment options come from the workspace_id → env mapping so the
    # multiselect works before any data has been fetched. Date bounds use
    # the configured lookback window (the SQL query won't return rows
    # outside it anyway).
    _env_options = sorted(set(DATABRICKS_WORKSPACE_TO_ENV.values()))
    _today = date.today()
    _date_min = _today - timedelta(days=DATABRICKS_LOOKBACK_DAYS)

    _f1, _f2, _f3 = st.columns([2, 1, 1])
    with _f1:
        dbx_envs = st.multiselect(
            "Environment",
            options=_env_options,
            default=_env_options,
            key="dbx_env_filter",
        )
    with _f2:
        dbx_start = st.date_input(
            "Start date",
            value=_today - timedelta(days=30),
            min_value=_date_min,
            max_value=_today,
            key="dbx_start_filter",
        )
    with _f3:
        dbx_end = st.date_input(
            "End date",
            value=_today,
            min_value=_date_min,
            max_value=_today,
            key="dbx_end_filter",
        )

    # Single button: force-refresh the warehouse query. Without a click the
    # @st.cache_data cache (10 min TTL) is used; with a click the cache is
    # cleared so the next read goes back to the warehouse.
    if st.button("Refresh data", key="dbx_load_btn"):
        load_databricks_jobs.clear()

    # Always fetch on page render. First call costs ~1–3 s against the
    # warehouse; subsequent reruns (filter changes, etc.) hit the cache.
    dbx_jobs = load_databricks_jobs()
    if dbx_jobs.empty:
        st.info("Databricks SQL warehouse returned no rows.")
    else:
        _dbx_full = dbx_jobs.copy()
        _dbx_full["usage_date"] = pd.to_datetime(_dbx_full["usage_date"]).dt.date
        _dbx_full["environment"] = (
            _dbx_full["workspace_id"].astype(str)
            .map(DATABRICKS_WORKSPACE_TO_ENV)
            .fillna("ws-" + _dbx_full["workspace_id"].astype(str))
        )
        # Drop SKUs that are pass-through line items tracked elsewhere
        # (Anthropic API, etc.) — see DATABRICKS_EXCLUDED_SKUS in config.
        if DATABRICKS_EXCLUDED_SKUS:
            _dbx_full = _dbx_full[~_dbx_full["sku_name"].isin(DATABRICKS_EXCLUDED_SKUS)]

        _dbx_window = _dbx_full
        if dbx_envs:
            _dbx_window = _dbx_window[_dbx_window["environment"].isin(dbx_envs)]
        if dbx_start and dbx_end:
            if dbx_start > dbx_end:
                st.warning("Start date is after end date — no rows will match.")
            _dbx_window = _dbx_window[
                (_dbx_window["usage_date"] >= dbx_start)
                & (_dbx_window["usage_date"] <= dbx_end)
            ]

        if _dbx_window.empty:
            st.info("No Databricks job data in the selected window / environment.")
        else:
            # Per-environment totals so you can see the split across workspaces.
            _by_env = (
                _dbx_window.groupby("environment", dropna=False)["cost_usd"]
                .sum().reset_index().sort_values("cost_usd", ascending=False)
            )
            env_totals_str = " · ".join(
                f"**{row['environment']}**: {_fmt_usd(row['cost_usd'])}"
                for _, row in _by_env.iterrows()
            )
            st.caption(f"By environment — {env_totals_str}")

            _by_workload = (
                _dbx_window.groupby(["environment", "workload"], dropna=False)
                .agg(
                    total_dbus=("total_dbus", "sum"),
                    cost_usd=("cost_usd", "sum"),
                    first_day=("usage_date", "min"),
                    last_day=("usage_date", "max"),
                )
                .reset_index()
                .sort_values("cost_usd", ascending=False)
            )
            total_dbx_spend = float(_by_workload["cost_usd"].sum())
            st.caption(
                f"Total DBU spend in window: **{_fmt_usd(total_dbx_spend)}** "
                f"across {len(_by_workload)} workloads."
            )

            _top = _by_workload.head(20).copy()
            _top["cost_usd"] = _top["cost_usd"].map(_fmt_usd)
            _top["total_dbus"] = _top["total_dbus"].map(lambda v: f"{v:,.2f}")
            _top["first_day"] = pd.to_datetime(_top["first_day"]).dt.strftime("%Y-%m-%d")
            _top["last_day"]  = pd.to_datetime(_top["last_day"]).dt.strftime("%Y-%m-%d")
            st.dataframe(_top, hide_index=True, use_container_width=True)

# ---- Anomaly table --------------------------------------------------------

st.subheader("Anomalies — latest day > 2× trailing 7-day average")
# Per (env, service_family): compute trailing 7d mean ending the day before
# the latest day, and flag if latest > 2x.
window_df = daily.copy()
window_df = window_df.dropna(subset=["environment", "service_family"])
latest_day = window_df["date"].max()
trailing_start = latest_day - timedelta(days=7)
trailing = (
    window_df[(window_df["date"] >= trailing_start) & (window_df["date"] < latest_day)]
    .groupby(["environment", "service_family"])["cost"]
    .mean()
    .rename("trailing_7d_avg")
)
latest = (
    window_df[window_df["date"] == latest_day]
    .groupby(["environment", "service_family"])["cost"]
    .sum()
    .rename("latest_day_cost")
)
anom = pd.concat([latest, trailing], axis=1).reset_index().dropna()
anom = anom[(anom["trailing_7d_avg"] > 0) & (anom["latest_day_cost"] > 2 * anom["trailing_7d_avg"])]
if env_choice != "all":
    anom = anom[anom["environment"] == env_choice]
if anom.empty:
    st.success(f"No anomalies on {latest_day} (latest data).")
else:
    anom["ratio"] = (anom["latest_day_cost"] / anom["trailing_7d_avg"]).map(lambda r: f"{r:.1f}×")
    anom["latest_day_cost"] = anom["latest_day_cost"].map(lambda v: f"${v:,.2f}")
    anom["trailing_7d_avg"] = anom["trailing_7d_avg"].map(lambda v: f"${v:,.2f}")
    st.dataframe(
        anom[["environment", "service_family", "latest_day_cost", "trailing_7d_avg", "ratio"]],
        hide_index=True,
        use_container_width=True,
    )
    st.caption(f"Anomalies for {latest_day}")
