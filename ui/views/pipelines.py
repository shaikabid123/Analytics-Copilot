import streamlit as st

import api_client as ac

ac.require_login()
st.title("Pipelines")
st.caption("Starting a run QUEUES it; the ETL worker picks it up. Only one run per pipeline can be queued or running.")
if st.session_state.get("flash"):
    kind, text = st.session_state.pop("flash")
    (st.success if kind == "success" else st.error)(text)
st.button("Refresh", key="refresh")

overview = ac.need(ac.api().get("/pipelines"))
for p in overview:
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns([2, 3, 3, 2])
        c1.markdown(f"**{p['name']}**  \n`{p['schedule_cron'] or 'manual'}`")
        last = p["last_run"]
        c2.markdown(f"Last run: **{last['status'] if last else 'never'}**" + (f" (#{last['id']})" if last else ""))
        fresh = f"{p['hours_since_success']}h ago" if p["hours_since_success"] is not None else "never succeeded"
        c3.markdown(f"Last success: {fresh}  \nSLA {p['sla_hours']:g}h: " + ("🔴 breached" if p["sla_breached"] else "🟢 ok"))
        busy = p["active_run"]
        if c4.button("Run now", key=f"run_{p['name']}", disabled=busy is not None):
            res = ac.api().post(f"/pipelines/{p['name']}/runs")
            st.session_state["flash"] = ("success", f"Queued run #{res.data['run_id']} for {p['name']}.") if res.ok else ("error", res.error)
            st.rerun()
        if busy:
            c4.caption(f"run #{busy['id']} is {busy['status']}")
        if p["overrides"]:
            st.caption("Approved overrides in effect: " + ", ".join(f"`{k}={v}`" for k, v in p["overrides"].items()))
            for param in p["overrides"]:
                if st.button(f"Revert {param} (admin)", key=f"revert_{p['name']}_{param}"):
                    res = ac.api().delete(f"/pipelines/{p['name']}/overrides/{param}")
                    st.session_state["flash"] = ("success", f"Reverted {param} on {p['name']}.") if res.ok else ("error", res.error)
                    st.rerun()

st.subheader("Run history")
f1, f2 = st.columns(2)
fp = f1.selectbox("Pipeline", ["(all)"] + [p["name"] for p in overview], key="hist_pipeline")
fs = f2.selectbox("Status", ["(all)", "queued", "running", "succeeded", "failed", "retrying"], key="hist_status")
runs = ac.need(ac.api().get("/pipelines/runs", limit=30, pipeline=None if fp == "(all)" else fp, status=None if fs == "(all)" else fs))
st.dataframe(ac.df(["run", "pipeline", "status", "try", "trigger", "rows in", "rows out", "finished", "error"],
                   [[r["id"], r["pipeline"], r["status"], r["attempt"], r["trigger"], r["rows_in"], r["rows_out"], r["finished_at"], (r["error"] or "")[:80]] for r in runs]),
             width="stretch", hide_index=True)
if runs:
    sel = st.selectbox("Select a run for recovery actions", runs, key="run_pick", format_func=lambda r: f"#{r['id']} · {r['pipeline']} · {r['status']}")
    if sel["error"]:
        st.error(sel["error"])
    a1, a2, a3 = st.columns(3)
    for col, label, key, path, show in (
            (a1, "Retry this run", "act_retry", f"/pipelines/runs/{sel['id']}/retry", sel["status"] in ("failed", "retrying")),
            (a2, "Cancel (queued only)", "act_cancel", f"/pipelines/runs/{sel['id']}/cancel", sel["status"] == "queued"),
            (a3, "Abandon stuck run (admin)", "act_abandon", f"/pipelines/runs/{sel['id']}/abandon", sel["status"] == "running")):
        if show and col.button(label, key=key):
            res = ac.api().post(path)
            st.session_state["flash"] = ("success", f"{label}: OK (run #{res.data['run_id']} → {res.data['status']}).") if res.ok else ("error", res.error)
            st.rerun()
