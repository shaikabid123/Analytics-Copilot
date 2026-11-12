import streamlit as st

import api_client as ac

ac.require_login()
st.title("Ask & SQL")
tab_ask, tab_sql = st.tabs(["Ask in plain language", "SQL console"])


def show_run(run_id: str, answer: str | None) -> None:
    run = ac.need(ac.api().get(f"/agents/runs/{run_id}"))
    state = run["state"] or {}
    st.markdown(f"**Status:** `{run['status']}` · **Intent:** `{run['intent']}` · run `{run_id[:8]}`")
    if run["status"] == "awaiting_approval":
        st.info(f"Approval #{state.get('approval_id')} is pending. An engineer or admin must decide it in the Approval queue. Nothing has been changed.")
    if answer and not state.get("sql_result"):
        st.markdown(answer)
    r = state.get("sql_result")
    if r:
        st.code(r["sql"], language="sql")
        table = ac.df(r["columns"], r["rows_preview"])
        st.dataframe(table, width="stretch", hide_index=True)
        if r["row_count"] > len(r["rows_preview"]):
            st.caption(f"Showing the first {len(r['rows_preview'])} of {r['row_count']} rows.")
        st.download_button("Download shown rows as CSV", table.to_csv(index=False), "result.csv", "text/csv", key="ask_csv")
        st.caption(f"source: {state.get('sql_source')} · attempts: {state.get('sql_attempts')} · {r['duration_ms']} ms · planner cost {r['estimated_cost']:,.0f}")
    elif state.get("guard_errors"):
        st.warning("No query was run: " + "; ".join(state["guard_errors"]))
    with st.expander("How this answer was produced"):
        st.write("Steps: " + " → ".join(state.get("steps", [])))
        if state.get("degraded_reasons"):
            st.warning("Fallbacks used: " + ", ".join(state["degraded_reasons"]) + " (the language model was unavailable or its SQL was rejected).")
        if state.get("verified") is not None:
            st.write(f"Verified: {state['verified']}")


with tab_ask:
    st.caption("Examples: *total sales by month in 2026* · *return rate by channel* · *average delivery time by carrier* · "
               "*what is gross margin* · *top 5 customers by lifetime value* (engineers/admins)")
    q = st.text_area("Your question", key="ask_q", max_chars=500, height=80)
    if st.button("Ask", key="ask_go"):
        if not q.strip():
            st.error("Enter a question first.")
        else:
            with st.spinner("Working on it…"):
                data = ac.need(ac.api().post("/agents/ask", {"question": q.strip()}))
            st.session_state["last_run"], st.session_state["last_answer"] = data["run_id"], data["answer"]
    if st.session_state.get("last_run"):
        show_run(st.session_state["last_run"], st.session_state.get("last_answer"))

with tab_sql:
    st.caption("Single read-only SELECT over the Gold tables. Writes, multiple statements and tables you may not read are refused by the server.")
    sql = st.text_area("SELECT statement", key="sql_text", max_chars=4000, height=140,
                       placeholder="SELECT channel, SUM(orders) AS orders FROM gold.daily_sales GROUP BY channel")
    c1, c2 = st.columns(2)
    if c1.button("Validate", key="sql_validate"):
        res = ac.api().post("/query/validate", {"sql": sql})
        if res.ok and res.data["ok"]:
            st.success("Allowed. This is what would run:")
            st.code(res.data["sql"], language="sql")
        elif res.ok:
            st.error("Refused by the server:")
            for e in res.data["errors"]:
                st.caption(f"• {e}")
        else:
            ac.render_error(res)
    if c2.button("Run", key="sql_run"):
        with st.spinner("Running…"):
            res = ac.api().post("/query/execute", {"sql": sql})
        if res.ok:
            table = ac.df(res.data["columns"], res.data["rows"])
            st.dataframe(table, width="stretch", hide_index=True)
            st.caption(f"{res.data['row_count']} row(s){' (truncated)' if res.data['truncated'] else ''} · {res.data['duration_ms']} ms")
            st.download_button("Download as CSV", table.to_csv(index=False), "query.csv", "text/csv", key="sql_csv")
        else:
            ac.render_error(res)
