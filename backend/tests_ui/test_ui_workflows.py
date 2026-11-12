"""Complete user workflows through the real Streamlit pages against the live secured API."""
import re
import uuid

import httpx
import pytest
from sqlalchemy import text

from app.agents import service
from app.llm import NullLLM
from tests_ui.conftest import REJECT_FAIL, run_view, select_by_label, texts


def frames(at):
    return [d.value for d in at.dataframe]


def frame_with(at, column):
    return next(f for f in frames(at) if column in f.columns)


def clean(at):
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]


# =============================================================================================== analyst
def test_catalog_search_browse_and_glossary(ui_user):
    _, tok = ui_user("analyst")
    at = run_view("catalog", tok)
    clean(at)
    at.text_input(key="catalog_q").set_value("customer email").run()
    assert "customers.email" in set(frame_with(at, "snippet")["name"])
    at.selectbox(key="catalog_layer").select("gold").run()
    datasets = frame_with(at, "dataset")
    assert set(datasets["dataset"]) == {"gold.daily_sales", "gold.customer_value", "gold.funnel_daily", "gold.delivery_sla"}
    assert (datasets["layer"] == "gold").all()
    assert any("Gross margin" in e.label for e in at.expander) and any("Return rate" in e.label for e in at.expander)
    at.text_input(key="catalog_q").set_value("x").run()
    assert "at least 2 characters" in texts(at)
    at.text_input(key="catalog_q").set_value("zzzzqq").run()
    assert "No matches" in texts(at)


def test_sources_page_summarises_each_source_without_exposing_connection_settings(ui_user):
    _, tok = ui_user("analyst")
    at = run_view("sources", tok)
    clean(at)
    table = frame_with(at, "last synced")
    by = table.set_index("source")
    assert {"retail_pg", "web_events_csv", "shipments_parquet", "gold_pg"} <= set(by.index)
    assert by.loc["retail_pg", "rows"] > 250_000 and by.loc["gold_pg", "datasets"] == 4 and by.loc["retail_pg", "type"] == "postgres"
    page = texts(at) + str(frames(at))
    for secret in ("password", "SRC_PG", "source_ro", "analytics_ro", "5432", "host"):
        assert secret not in page


def test_ask_in_plain_language_shows_the_sql_rows_and_how_it_was_produced(ui_user, ro_engine):
    _, tok = ui_user("analyst")
    at = run_view("query", tok)
    at.text_area(key="ask_q").set_value("total orders by channel")
    at.button(key="ask_go").click().run()
    clean(at)
    sql = [c.value for c in at.code][0]
    assert "gold.daily_sales" in sql and "LIMIT 1000" in sql
    table = frame_with(at, "channel")
    with ro_engine.connect() as c:
        direct = dict(c.execute(text("SELECT channel, SUM(orders)::bigint FROM gold.daily_sales GROUP BY channel")).all())
    assert {r.channel: int(r.orders) for r in table.itertuples()} == {k: int(v) for k, v in direct.items()}
    assert "intent" in texts(at).lower() and "Steps:" in texts(at) and "Fallbacks used" in texts(at)     # evidence + degraded mode shown


def test_ask_refusals_are_explained_not_hidden(ui_user):
    _, tok = ui_user("analyst")
    at = run_view("query", tok)
    at.button(key="ask_go").click().run()
    assert "Enter a question first" in texts(at)
    at.text_area(key="ask_q").set_value("top 5 customers by lifetime value")
    at.button(key="ask_go").click().run()
    assert not at.exception and "not permitted for role 'analyst'" in texts(at) and not at.code
    _, eng = ui_user("engineer")
    at2 = run_view("query", eng)
    at2.text_area(key="ask_q").set_value("top 5 customers by lifetime value")
    at2.button(key="ask_go").click().run()
    assert "lifetime_value" in str(frames(at2)) and len(frame_with(at2, "lifetime_value")) == 5


def test_sql_console_runs_selects_and_refuses_everything_else(ui_user, ro_engine):
    _, tok = ui_user("analyst")
    at = run_view("query", tok)
    at.text_area(key="sql_text").set_value("SELECT channel, SUM(orders) AS orders FROM gold.daily_sales GROUP BY channel")
    at.button(key="sql_validate").click().run()
    assert "Allowed" in texts(at) and any("LIMIT 1000" in c.value for c in at.code)
    at.button(key="sql_run").click().run()
    assert len(frame_with(at, "orders")) == 3 and "3 row(s)" in texts(at)
    with ro_engine.connect() as c:
        before = c.execute(text("SELECT count(*) FROM gold.daily_sales")).scalar()
    for attack, fragment in (("DELETE FROM gold.daily_sales", "SELECT"), ("DROP TABLE gold.daily_sales", "SELECT"),
                             ("SELECT 1; DROP TABLE gold.daily_sales", "exactly one"), ("SELECT customer_id FROM gold.customer_value", "not permitted for role"),
                             ("SELECT password_hash FROM app.users", "not allowed"), ("SELECT pg_sleep(20)", "not allowed")):
        at.text_area(key="sql_text").set_value(attack)
        at.button(key="sql_run").click().run()
        assert not at.exception and "Not permitted" in texts(at) and fragment in texts(at), (attack, texts(at)[-300:])
    at.text_area(key="sql_text").set_value("DELETE FROM gold.daily_sales")
    at.button(key="sql_validate").click().run()
    assert "Refused by the server" in texts(at)
    with ro_engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM gold.daily_sales")).scalar() == before          # nothing was deleted


def test_analytics_dashboards_render_and_match_the_database(ui_user, ro_engine):
    _, tok = ui_user("analyst")
    at = run_view("analytics", tok)
    clean(at)
    assert len(at.get("vega_lite_chart")) == 1
    gmv_shown = int(re.sub(r"[^0-9]", "", at.metric[0].value))
    with ro_engine.connect() as c:
        total = c.execute(text("SELECT SUM(gross_merchandise_value) FROM gold.daily_sales")).scalar()
        web = c.execute(text("SELECT SUM(gross_merchandise_value) FROM gold.daily_sales WHERE channel='web'")).scalar()
    assert gmv_shown == round(float(total))
    at.selectbox(key="board_channel").select("web").run()
    assert int(re.sub(r"[^0-9]", "", at.metric[0].value)) == round(float(web))
    for board, chart in (("Channels", "arrow_bar_chart"), ("Web funnel", "arrow_bar_chart"), ("Delivery SLA", "arrow_bar_chart")):
        at.selectbox(key="board").select(board).run()
        clean(at)
        assert len(at.get("vega_lite_chart")) == 1 and len(at.dataframe) >= 1, board
    at.selectbox(key="board").select("Top customers").run()
    assert "Not permitted" in texts(at) and not at.exception                                       # analysts are refused by the server


def test_engineers_can_see_customer_level_analytics(ui_user):
    _, tok = ui_user("engineer")
    at = run_view("analytics", tok)
    at.selectbox(key="board").select("Top customers").run()
    clean(at)
    assert len(frame_with(at, "lifetime_value")) == 20


# =============================================================================================== engineer: pipelines
def db_run(app_engine, rid):
    with app_engine.connect() as c:
        return c.execute(text("SELECT status, trigger, retry_of_run_id, error_message FROM app.pipeline_runs WHERE id=:i"), {"i": rid}).one()


def newest_run(app_engine, name):
    with app_engine.connect() as c:
        return c.execute(text("SELECT id FROM app.pipeline_runs WHERE pipeline_id=(SELECT id FROM app.pipelines WHERE name=:n) ORDER BY id DESC LIMIT 1"), {"n": name}).scalar()


def test_run_cancel_and_retry_cycle_through_the_pipelines_page(ui_user, pipe, app_engine):
    _, tok = ui_user("engineer")
    at = run_view("pipelines", tok)
    assert not at.exception                                              # (a failed run's error text is expected content)
    assert not at.button(key=f"run_{pipe.name}").disabled and "SLA 26h" in texts(at)
    at.button(key=f"run_{pipe.name}").click().run()
    assert "Queued run #" in texts(at)
    queued = newest_run(app_engine, pipe.name)
    assert db_run(app_engine, queued)[0] == "queued"
    at = run_view("pipelines", tok)
    assert at.button(key=f"run_{pipe.name}").disabled and f"run #{queued} is queued" in texts(at)      # duplicate start is impossible
    at.selectbox(key="hist_pipeline").select(pipe.name).run()
    select_by_label(at, "run_pick", f"#{queued}").run()
    at.button(key="act_cancel").click().run()
    assert "OK" in texts(at) and db_run(app_engine, queued)[0] == "failed" and "cancelled by" in db_run(app_engine, queued)[3]
    at = run_view("pipelines", tok)
    at.selectbox(key="hist_pipeline").select(pipe.name).run()
    select_by_label(at, "run_pick", f"#{pipe.failed_run}").run()
    assert "DQGateError" in texts(at)                                                                    # the failure reason is visible
    at.button(key="act_retry").click().run()
    retry = newest_run(app_engine, pipe.name)
    assert retry not in (queued, pipe.failed_run) and db_run(app_engine, retry)[:3] == ("queued", "retry", pipe.failed_run)


def test_abandoning_a_stuck_run_is_admin_only_and_the_ui_shows_the_refusal(ui_user, pipe, app_engine):
    _, eng = ui_user("engineer")
    _, adm = ui_user("admin")
    at = run_view("pipelines", eng)
    at.button(key=f"run_{pipe.name}").click().run()
    stuck = newest_run(app_engine, pipe.name)
    with app_engine.begin() as c:
        c.execute(text("UPDATE app.pipeline_runs SET status='running', started_at=now() WHERE id=:i"), {"i": stuck})
    at = run_view("pipelines", eng)
    at.selectbox(key="hist_pipeline").select(pipe.name).run()
    select_by_label(at, "run_pick", f"#{stuck}").run()
    at.button(key="act_abandon").click().run()
    assert "Not permitted" in texts(at) and db_run(app_engine, stuck)[0] == "running"                  # backend refused the engineer
    at = run_view("pipelines", adm)
    at.selectbox(key="hist_pipeline").select(pipe.name).run()
    select_by_label(at, "run_pick", f"#{stuck}").run()
    at.button(key="act_abandon").click().run()
    assert db_run(app_engine, stuck)[0] == "failed" and "abandoned by admin" in db_run(app_engine, stuck)[3]


def test_overrides_are_visible_and_only_admins_can_revert_them(ui_user, pipe, app_engine):
    with app_engine.begin() as c:
        c.execute(text("UPDATE app.pipelines SET params = CAST('{\"max_reject_pct\": 6.0}' AS jsonb) WHERE name=:n"), {"n": pipe.name})
    _, eng = ui_user("engineer")
    _, adm = ui_user("admin")
    key = f"revert_{pipe.name}_max_reject_pct"
    at = run_view("pipelines", eng)
    assert "max_reject_pct=6.0" in texts(at)
    at.button(key=key).click().run()
    assert "Not permitted" in texts(at)
    at = run_view("pipelines", adm)
    at.button(key=key).click().run()
    with app_engine.connect() as c:
        assert c.execute(text("SELECT params FROM app.pipelines WHERE name=:n"), {"n": pipe.name}).scalar() == {}


# =============================================================================================== engineer: quality, approvals, monitoring
def test_quality_page_explains_a_failed_run(ui_user, pipe):
    _, tok = ui_user("engineer")
    at = run_view("quality", tok)
    at.selectbox(key="q_pipeline").select(pipe.name).run()
    assert not at.exception
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["Blocking failures"] == "1" and metrics["Checks"] == "2"
    page = texts(at)
    assert "DQGateError" in page and "Gold was NOT published" in page and "customers.reject_rate" in page
    assert len(at.get("vega_lite_chart")) == 1                                                          # reject reasons chart
    drift = frame_with(at, "PSI")
    assert drift.iloc[0]["column"] == "orders.total_amount" and drift.iloc[0]["PSI"] == 0.6 and drift.iloc[0]["zone"] == "major"
    assert "No alerts were raised" in page and "at least two finished runs" in page


def test_alert_delivery_from_the_quality_page_succeeds_and_degrades_gracefully(ui_user, pipe, app_engine, mock_integrations):
    with app_engine.begin() as c:
        aid = c.execute(text("INSERT INTO app.dq_alerts (run_id, severity, title, detail) VALUES (:r, 'medium', '[t] customers.reject_rate failed', "
                             "CAST('{\"observed\": 4.0, \"threshold\": 2.0}' AS jsonb)) RETURNING id"), {"r": pipe.failed_run}).scalar()
    _, tok = ui_user("engineer")
    mock_integrations.control("slack", "fail")
    at = run_view("quality", tok)
    at.selectbox(key="q_pipeline").select(pipe.name).run()
    assert "[t] customers.reject_rate failed" in str(frames(at))
    at.button(key="q_dispatch").click().run()
    assert "integration looks unavailable" in texts(at) and not at.exception
    with app_engine.connect() as c:
        assert c.execute(text("SELECT status, attempts FROM app.dq_alerts WHERE id=:i"), {"i": aid}).one() == ("pending", 1)
    mock_integrations.control("slack", "ok")
    with app_engine.begin() as c:
        c.execute(text("UPDATE app.dq_alerts SET next_attempt_at = now() WHERE id=:i"), {"i": aid})
    at.button(key="q_dispatch").click().run()
    assert "Sent" in texts(at) and not at.exception
    with app_engine.connect() as c:
        assert c.execute(text("SELECT status FROM app.dq_alerts WHERE id=:i"), {"i": aid}).scalar() == "sent"
    assert any("customers.reject_rate failed" in m["text"] for m in mock_integrations.state()["slack"]["received"])


def test_approval_workflow_through_the_ui(ui_user, make_run, app_engine):
    requester, rtok = ui_user("engineer")
    reviewer, vtok = ui_user("engineer")
    _, atok = ui_user("analyst")
    pname, rid = make_run(checks=[REJECT_FAIL], error="DQGateError: gate failed")
    out = service.run_request(f"why did run {rid} fail", requester, "engineer", llm=NullLLM())
    aid = out["approval_id"]
    params = lambda: app_engine.connect().execute(text("SELECT params FROM app.pipelines WHERE name=:n"), {"n": pname}).scalar()   # noqa: E731

    at = run_view("approvals", rtok)
    clean(at)
    assert any(f"#{aid}" in e.label and "pending" in e.label for e in at.expander)
    page = texts(at)
    assert "set_max_reject_pct" in page and "Risk:" in page and requester in page
    at.button(key=f"approved_{aid}").click().run()                                                      # the requester tries to approve
    assert "cannot approve or reject a change that you requested" in texts(at) and params() == {}

    forced = run_view("approvals", atok)                                                                # an analyst reaching the page
    assert "Not permitted" in texts(forced) and not [b for b in forced.button if "approved_" in (b.key or "")]

    at = run_view("approvals", vtok)
    at.text_input(key=f"note_{aid}").set_value("reviewed in UI test")
    at.button(key=f"approved_{aid}").click().run()
    assert f"Approval #{aid} approved" in texts(at) and params() == {"max_reject_pct": 5.0}
    with app_engine.connect() as c:
        assert c.execute(text("SELECT status, decided_by_label, decision_note FROM app.approvals WHERE id=:i"), {"i": aid}).one() == ("approved", reviewer, "reviewed in UI test")
    at.radio(key="ap_status").set_value("approved").run()
    assert any(f"#{aid}" in e.label for e in at.expander) and "Applied at" in texts(at)


def test_rejecting_through_the_ui_changes_nothing(ui_user, make_run, app_engine):
    requester, _ = ui_user("engineer")
    reviewer, vtok = ui_user("admin")
    pname, rid = make_run(checks=[REJECT_FAIL], error="DQGateError: gate failed")
    aid = service.run_request(f"why did run {rid} fail", requester, "engineer", llm=NullLLM())["approval_id"]
    at = run_view("approvals", vtok)
    at.button(key=f"rejected_{aid}").click().run()
    assert f"Approval #{aid} rejected" in texts(at)
    with app_engine.connect() as c:
        assert c.execute(text("SELECT params FROM app.pipelines WHERE name=:n"), {"n": pname}).scalar() == {}
        assert c.execute(text("SELECT status FROM app.approvals WHERE id=:i"), {"i": aid}).scalar() == "rejected"


def test_monitoring_page_shows_execution_metrics_and_sla(ui_user):
    service.run_request("total orders by channel", "monitor_tester", "analyst", llm=NullLLM())
    _, tok = ui_user("engineer")
    at = run_view("monitoring", tok)
    clean(at)
    labels = {m.label for m in at.metric}
    assert {"Runs", "Error rate", "p50 latency", "p95 latency", "Calls", "Avg tokens / task", "Avg cost / task", "Blocked rate"} <= labels
    pipelines = frame_with(at, "hours since success")
    assert set(pipelines["pipeline"]) == {"sales", "events"} and set(pipelines["SLA"]) <= {"🟢 ok", "🔴 breached"}
    assert "this API process only" in texts(at) and "route" in "".join(str(f.columns) for f in frames(at))
    at.select_slider(key="mon_days").set_value(1).run()
    clean(at)


# =============================================================================================== admin
def test_admin_manages_users_and_reads_audit_trails(ui_user, app_engine):
    admin_name, tok = ui_user("admin")
    new = f"t_ui_{uuid.uuid4().hex[:8]}"
    try:
        at = run_view("admin", tok)
        clean(at)
        assert admin_name in set(frame_with(at, "locked")["username"])
        create = lambda: next(b for b in at.button if b.label == "Create")   # noqa: E731
        at.text_input(key="nu_name").set_value(new)
        at.text_input(key="nu_pw").set_value("short")
        create().click().run()
        assert "at least 12 characters" in texts(at)
        at.text_input(key="nu_pw").set_value("Strong-Passphrase-7")
        at.selectbox(key="nu_role").select("analyst")
        create().click().run()
        assert f"Created {new} (analyst)" in texts(at)
        create().click().run()
        assert "already exists" in texts(at)
        at.selectbox(key="mu_pick").select(new)
        at.button(key="mu_deactivate").click().run()
        assert f"Deactivated {new}" in texts(at)
        assert httpx.post(f"{__import__('os').environ['API_URL']}/auth/login", json={"username": new, "password": "Strong-Passphrase-7"}).status_code == 401
        at.text_input(key="au_action").set_value("user.create").run()
        assert (frame_with(at, "outcome")["action"] == "user.create").all() and new not in "" and len(frame_with(at, "outcome")) >= 1
        sql_audit = frame_with(at, "reasons")
        assert {"at", "actor", "role", "status", "source", "sql"} <= set(sql_audit.columns)
    finally:
        with app_engine.begin() as c:
            c.execute(text("DELETE FROM app.users WHERE username LIKE 't\\_ui\\_%'"))
