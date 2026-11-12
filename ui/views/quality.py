import streamlit as st

import api_client as ac

ac.require_login()
st.title("Quality & drift")
names = [p["name"] for p in ac.need(ac.api().get("/pipelines"))]
pipeline = st.selectbox("Pipeline", names, key="q_pipeline")
runs = ac.need(ac.api().get("/pipelines/runs", pipeline=pipeline, limit=20))
if not runs:
    st.info("This pipeline has no runs yet.")
    st.stop()
run = st.selectbox("Run", runs, key="q_run", format_func=lambda r: f"#{r['id']} · {r['status']} · {r['finished_at'] or 'not finished'}")
checks = ac.need(ac.api().get(f"/pipelines/runs/{run['id']}/checks"))
failed = [c for c in checks if c["status"] == "failed"]
blocking = [c for c in failed if c["severity"] == "error"]
warnings = [c for c in checks if c["status"] == "warning"]
m1, m2, m3, m4 = st.columns(4)
m1.metric("Checks", len(checks)); m2.metric("Passed", sum(1 for c in checks if c["status"] == "passed"))
m3.metric("Blocking failures", len(blocking)); m4.metric("Warnings / non-blocking", len(warnings) + len(failed) - len(blocking))
if run["error"]:
    st.error(run["error"])
if blocking:
    st.warning("Gold was NOT published for this run because of: " + ", ".join(f"{c['dataset']}.{c['check']}" for c in blocking))

show = st.multiselect("Show statuses", ["failed", "warning", "passed"], default=["failed", "warning"], key="q_status")
rows = [[c["dataset"], c["check"], c["type"], c["severity"], c["status"], c["observed"], c["threshold"]] for c in checks if c["status"] in show]
st.dataframe(ac.df(["dataset", "check", "type", "severity", "status", "observed", "threshold"], rows), width="stretch", hide_index=True)

reasons = [(c["dataset"], c["detail"].get("reasons")) for c in checks if c["check"] == "reject_rate" and c["detail"].get("reasons")]
if reasons:
    st.subheader("Why rows were rejected")
    for dataset, rs in reasons:
        st.write(f"**{dataset}**")
        st.bar_chart(ac.df(["reason", "rows"], list(rs.items())).set_index("reason"))

drift = [c for c in checks if c["type"] == "drift" and c["observed"] is not None]
st.subheader("Drift (PSI vs baseline)")
if drift:
    st.dataframe(ac.df(["column", "PSI", "zone"], [[f"{c['dataset']}.{c['check'][6:]}", round(c["observed"], 4),
                                                     "major" if c["status"] == "failed" else ("moderate" if c["status"] == "warning" else "stable")] for c in drift]),
                 width="stretch", hide_index=True)
    st.caption("PSI < 0.10 stable · 0.10–0.25 moderate · ≥ 0.25 major (alerts, does not block Gold).")
else:
    st.info("No drift scores for this run (the first successful run only creates the baseline).")

st.subheader("Alerts for this run")
alerts = [a for a in ac.need(ac.api().get("/integrations/alerts", limit=200)) if a["run_id"] == run["id"]]
if alerts:
    st.dataframe(ac.df(["severity", "title", "status", "attempts", "ticket", "last error"],
                       [[a["severity"], a["title"], a["status"], a["attempts"], a["ticket_key"], a["last_error"]] for a in alerts]), width="stretch", hide_index=True)
else:
    st.caption("No alerts were raised.")
if st.button("Deliver pending alerts now", key="q_dispatch"):
    res = ac.api().post("/integrations/dispatch")
    if res.ok:
        d = res.data
        (st.success if not d["degraded"] else st.warning)(f"Sent {d['sent']}, retry later {d['retry_later']}, failed {d['failed']}" +
                                                         ("; the integration looks unavailable, alerts stay queued." if d["degraded"] else "."))
    else:
        ac.render_error(res)

st.subheader("Trend")
trend = ac.need(ac.api().get("/pipelines/quality/summary", pipeline=pipeline, limit=20))["runs"]
if len(trend) >= 2:
    rr = {}
    for r in trend:
        for ds, v in r["reject_rate"].items():
            rr.setdefault(ds, {})[r["run_id"]] = v
    if rr:
        st.caption("Reject rate (%) per run")
        st.line_chart(ac.df(["run"] + list(rr), [[r["run_id"]] + [rr[d].get(r["run_id"]) for d in rr] for r in trend]).set_index("run"))
    psi = {}
    for r in trend:
        for col, v in r["drift_psi"].items():
            psi.setdefault(col, {})[r["run_id"]] = v
    if psi:
        st.caption("Drift PSI per run")
        st.line_chart(ac.df(["run"] + list(psi), [[r["run_id"]] + [psi[c].get(r["run_id"]) for c in psi] for r in trend]).set_index("run"))
else:
    st.caption("A trend needs at least two finished runs.")
