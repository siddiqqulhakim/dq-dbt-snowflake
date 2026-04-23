import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

st.set_page_config(page_title="dbt Quality Dashboard", layout="wide")

session = get_active_session()

RESULTS_TABLE = "DEMO_ENTERPRISE_DBT_DB.PUBLIC.DBT_TEST_RESULTS"


@st.cache_data(ttl=60)
def load_data():
    return session.sql(f"SELECT * FROM {RESULTS_TABLE} ORDER BY run_started_at DESC").to_pandas()


@st.cache_data(ttl=60)
def get_runs():
    return session.sql(
        f"SELECT DISTINCT run_started_at FROM {RESULTS_TABLE} ORDER BY run_started_at DESC"
    ).to_pandas()


st.title("dbt Test Quality Dashboard")

runs_df = get_runs()

if runs_df.empty:
    st.warning("No test results found. Run `dbt build` or `dbt test` to populate results.")
    st.stop()

run_options = runs_df["RUN_STARTED_AT"].tolist()

col_filter1, col_filter2 = st.columns(2)
with col_filter1:
    selected_run = st.selectbox("Select Run", run_options, index=0)
with col_filter2:
    compare_run = st.selectbox("Compare With (optional)", [None] + run_options[1:], index=0)

df_all = load_data()
df = df_all[df_all["RUN_STARTED_AT"] == selected_run].copy()

total = len(df)
passed = len(df[df["STATUS"] == "pass"])
failed = len(df[df["STATUS"] == "fail"])
skipped = len(df[df["STATUS"] == "skipped"])
errored = len(df[df["STATUS"] == "error"])
pass_rate = round(passed / total * 100, 1) if total > 0 else 0

st.markdown("---")

m1, m2, m3, m4, m5 = st.columns(5)

if compare_run:
    df_prev = df_all[df_all["RUN_STARTED_AT"] == compare_run]
    prev_total = len(df_prev)
    prev_passed = len(df_prev[df_prev["STATUS"] == "pass"])
    prev_failed = len(df_prev[df_prev["STATUS"] == "fail"])
    prev_rate = round(prev_passed / prev_total * 100, 1) if prev_total > 0 else 0
    m1.metric("Pass Rate", f"{pass_rate}%", delta=f"{round(pass_rate - prev_rate, 1)}%")
    m2.metric("Total Tests", total, delta=total - prev_total)
    m3.metric("Passed", passed, delta=passed - prev_passed)
    m4.metric("Failed", failed, delta=failed - prev_failed, delta_color="inverse")
    m5.metric("Skipped", skipped)
else:
    m1.metric("Pass Rate", f"{pass_rate}%")
    m2.metric("Total Tests", total)
    m3.metric("Passed", passed)
    m4.metric("Failed", failed)
    m5.metric("Skipped", skipped)

st.markdown("---")

left_col, right_col = st.columns(2)

with left_col:
    st.subheader("Pass Rate by Layer")
    layer_df = (
        df.groupby("LAYER")
        .apply(lambda g: pd.Series({
            "Total": len(g),
            "Passed": len(g[g["STATUS"] == "pass"]),
            "Failed": len(g[g["STATUS"] == "fail"]),
            "Pass Rate %": round(len(g[g["STATUS"] == "pass"]) / len(g) * 100, 1) if len(g) > 0 else 0,
        }))
        .reset_index()
    )
    layer_df.columns = ["Layer", "Total", "Passed", "Failed", "Pass Rate %"]
    st.dataframe(layer_df.set_index("Layer"), use_container_width=True)
    st.bar_chart(layer_df.set_index("Layer")["Pass Rate %"])

with right_col:
    st.subheader("Pass Rate by Domain")
    domain_df = (
        df.groupby("DOMAIN")
        .apply(lambda g: pd.Series({
            "Total": len(g),
            "Passed": len(g[g["STATUS"] == "pass"]),
            "Failed": len(g[g["STATUS"] == "fail"]),
            "Pass Rate %": round(len(g[g["STATUS"] == "pass"]) / len(g) * 100, 1) if len(g) > 0 else 0,
        }))
        .reset_index()
    )
    domain_df.columns = ["Domain", "Total", "Passed", "Failed", "Pass Rate %"]
    st.dataframe(domain_df.set_index("Domain"), use_container_width=True)
    st.bar_chart(domain_df.set_index("Domain")["Pass Rate %"])

st.markdown("---")

fail_df = df[df["STATUS"] != "pass"][
    ["MODEL_NAME", "TEST_NAME", "COLUMN_NAME", "TEST_TYPE", "STATUS", "FAILURES", "EXECUTION_TIME_S"]
].sort_values("FAILURES", ascending=False)

if not fail_df.empty:
    st.subheader(f"Failing / Skipped Tests ({len(fail_df)})")
    st.dataframe(fail_df.reset_index(drop=True), use_container_width=True)
else:
    st.success("All tests passed!")

st.markdown("---")
st.subheader("Pass Rate Trend Over Time")

trend_df = (
    df_all.groupby("RUN_STARTED_AT")
    .apply(lambda g: pd.Series({
        "Pass Rate %": round(len(g[g["STATUS"] == "pass"]) / len(g) * 100, 1) if len(g) > 0 else 0,
        "Total": len(g),
        "Failed": len(g[g["STATUS"] == "fail"]),
    }))
    .reset_index()
    .sort_values("RUN_STARTED_AT")
)
trend_df.columns = ["Run", "Pass Rate %", "Total", "Failed"]
st.line_chart(trend_df.set_index("Run")["Pass Rate %"])

st.markdown("---")
st.subheader("Slowest Tests (Top 10)")

slow_df = (
    df.nlargest(10, "EXECUTION_TIME_S")[["TEST_NAME", "MODEL_NAME", "EXECUTION_TIME_S"]]
    .copy()
)
slow_df["EXECUTION_TIME_S"] = slow_df["EXECUTION_TIME_S"].round(3)
slow_df.columns = ["Test", "Model", "Time (s)"]
st.dataframe(slow_df.reset_index(drop=True), use_container_width=True)

st.markdown("---")
st.subheader("Test Coverage per Model")

model_df = (
    df.groupby("MODEL_NAME")
    .apply(lambda g: pd.Series({
        "Tests": len(g),
        "Passed": len(g[g["STATUS"] == "pass"]),
        "Failed": len(g[g["STATUS"] == "fail"]),
    }))
    .reset_index()
    .sort_values("Failed", ascending=False)
)
model_df.columns = ["Model", "Tests", "Passed", "Failed"]
st.dataframe(model_df.reset_index(drop=True), use_container_width=True)
