import streamlit as st

import api_client as ac

ac.require_login()
st.title("Monitoring")
days = st.select_slider("Window (days)", [1, 3, 7, 14, 30, 90], value=7, key="mon_days")
st.button("Refresh", key="mon_refresh")
m = ac.need(ac.api().get("/metrics/summary", days=days))
pct = lambda v: "n/a" if v is None else f"{v * 100:.1f}%"      # noqa: E731
ms = lambda v: "n/a" if v is None else f"{v:,.0f} ms"          # noqa: E731

st.subheader("Agent workflow")
w = m["agent_workflow"]
c = st.columns(5)
c[0].metric("Runs", w["runs"]); c[1].metric("Error rate", pct(w["error_rate"])); c[2].metric("p50 latency", ms(w["latency_p50_ms"]))
c[3].metric("p95 latency", ms(w["latency_p95_ms"])); c[4].metric("Awaiting approval", w["awaiting_approval"])
if w["by_intent"]:
    st.dataframe(ac.df(["intent", "runs", "failed", "p50 ms", "p95 ms"], [[i["intent"], i["runs"], i["failed"], i["p50_ms"], i["p95_ms"]] for i in w["by_intent"]]), width="stretch", hide_index=True)

st.subheader("Language model")
l = m["llm"]
c = st.columns(5)
c[0].metric("Calls", l["calls"]); c[1].metric("Error rate", pct(l["error_rate"])); c[2].metric("p95 latency", ms(l["latency_p95_ms"]))
c[3].metric("Avg tokens / task", "n/a" if l["avg_tokens_per_task"] is None else l["avg_tokens_per_task"])
c[4].metric("Avg cost / task", "n/a" if l["avg_cost_usd_per_task"] is None else f"${l['avg_cost_usd_per_task']:.6f}")
st.caption(l["cost_note"] + f" · timeouts: {l['timeouts']}")

st.subheader("SQL safety")
s = m["sql"]
c = st.columns(4)
c[0].metric("Attempts", s["attempts"]); c[1].metric("Blocked rate", pct(s["blocked_rate"])); c[2].metric("Executed p50", ms(s["executed_p50_ms"])); c[3].metric("Executed p95", ms(s["executed_p95_ms"]))
if s["by_status"]:
    st.bar_chart(ac.df(["status", "count"], list(s["by_status"].items())).set_index("status"))

st.subheader("Pipelines (freshness SLA)")
st.dataframe(ac.df(["pipeline", "finished", "success rate", "failed", "retried attempts", "queued", "running", "p50 s", "p95 s", "hours since success", "SLA h", "SLA"],
                   [[p["pipeline"], p["finished_runs"], pct(p["success_rate"]), p["failed"], p["retried_attempts"], p["queued"], p["running"], p["duration_p50_s"],
                     p["duration_p95_s"], p["hours_since_success"], p["sla_hours"], "🔴 breached" if p["sla_breached"] else "🟢 ok"] for p in m["pipelines"]]),
             width="stretch", hide_index=True)

st.subheader("Integrations and alerts")
if m["integrations"]:
    st.dataframe(ac.df(["integration", "calls", "success rate", "timeouts", "p50 ms", "p95 ms", "last failure"],
                       [[i["integration"], i["calls"], pct(i["success_rate"]), i["timeouts"], i["latency_p50_ms"], i["latency_p95_ms"], i["last_failure_at"]] for i in m["integrations"]]),
                 width="stretch", hide_index=True)
else:
    st.caption("No integration calls in this window.")
st.write("Alerts by status: " + (", ".join(f"{k}: {v}" for k, v in m["alerts"].items()) or "none"))

st.subheader("API latency (this API process only; resets on restart)")
api_m = m["api"]
st.caption(f"Overall: {api_m['overall']['requests']} requests · 5xx rate {pct(api_m['overall']['error_rate'])} · p50 {ms(api_m['overall']['p50_ms'])} · p95 {ms(api_m['overall']['p95_ms'])}")
st.dataframe(ac.df(["route", "requests", "4xx", "5xx", "p50 ms", "p95 ms"],
                   [[k, v["requests"], v["client_errors_4xx"], v["errors_5xx"], v["p50_ms"], v["p95_ms"]] for k, v in api_m["routes"].items()]), width="stretch", hide_index=True)
