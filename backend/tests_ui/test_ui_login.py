"""Login gate, session handling and role-based navigation (Home.py)."""
import httpx
from unittest import mock

import pytest

from tests_ui.conftest import PASSWORD, has_token, home, texts


def login(at, username, password):
    at.text_input(key="login_user").set_value(username)
    at.text_input(key="login_pw").set_value(password)
    at.button[0].click().run()
    return at


def nav_for(token):
    """Run Home.py with a session token and capture the navigation the user is offered."""
    captured = {}
    with mock.patch("streamlit.navigation") as nav:
        nav.side_effect = lambda pages, **kw: (captured.update(pages=pages), mock.MagicMock())[1]
        home(token)
    return {k: [p.title for p in v] for k, v in captured.get("pages", {}).items()}


def test_logged_out_visitors_see_only_the_login_form(live_api):
    at = home()
    assert "AI Data Engineering" in texts(at) and [t.key for t in at.text_input] == ["login_user", "login_pw"]
    assert not has_token(at) and len(at.sidebar.markdown) == 0 and len(at.dataframe) == 0


def test_wrong_credentials_and_empty_input_are_rejected_with_clear_messages(live_api, users):
    name, pw = users("analyst")
    at = login(home(), name, "definitely-wrong-password")
    assert "Invalid username or password" in texts(at) and not has_token(at)
    at2 = login(home(), "", "")
    assert "Enter both username and password" in texts(at2) and not has_token(at2)
    ghost = login(home(), "no_such_user", "whatever-password-1")
    assert texts(ghost).count("Invalid username or password") == 1               # same message as a wrong password: no user enumeration


def test_successful_login_stores_a_token_in_server_side_session_only(live_api, users):
    name, pw = users("engineer")
    at = login(home(), name, pw)
    assert has_token(at) and f"role: `engineer`" in at.sidebar.markdown[0].value and name in at.sidebar.markdown[0].value
    assert "Data catalog & glossary" in texts(at)                                  # lands on the first page
    assert dict(at.query_params) == {}                                              # never in the URL
    state = at.session_state.to_dict()                                              # everything the session holds
    assert PASSWORD not in str(state) and not state.get("login_pw")
    assert set(state) >= {"token", "username", "role"} and state["role"] == "engineer"


def test_navigation_matches_the_role_and_is_decided_from_the_backend_role(live_api, ui_user):
    explore = ["Catalog & glossary", "Data sources", "Ask & SQL", "Gold analytics"]
    operate = ["Pipelines", "Quality & drift", "Approval queue", "Monitoring"]
    _, analyst = ui_user("analyst")
    _, engineer = ui_user("engineer")
    _, admin = ui_user("admin")
    assert nav_for(analyst) == {"Explore": explore}
    assert nav_for(engineer) == {"Explore": explore, "Operate": operate}
    assert nav_for(admin) == {"Explore": explore, "Operate": operate, "Administer": ["Users & audit"]}


def test_role_changes_take_effect_on_the_next_interaction(live_api, ui_user, users):
    admin_name, admin_tok = ui_user("admin")
    name, tok = ui_user("engineer")
    assert "Operate" in nav_for(tok)
    r = httpx.patch(f"{live_api}/admin/users/{name}", json={"role": "analyst"}, headers={"Authorization": f"Bearer {admin_tok}"})
    assert r.status_code == 200
    assert "Operate" not in nav_for(tok)                                            # same token, new backend role


def test_logout_clears_the_session(live_api, users):
    name, pw = users("analyst")
    at = login(home(), name, pw)
    assert has_token(at)
    at.button(key="logout").click().run()
    assert not has_token(at) and [t.key for t in at.text_input] == ["login_user", "login_pw"]


@pytest.mark.parametrize("bad_token", ["abc.def.ghi", "", "not-a-jwt"])
def test_forged_tokens_in_the_session_send_the_user_back_to_login(live_api, bad_token):
    at = home(bad_token or "x")
    assert not has_token(at) and "session expired" in texts(at)
    assert [t.key for t in at.text_input] == ["login_user", "login_pw"]


def test_tampered_token_is_rejected_by_the_server(live_api, ui_user):
    _, tok = ui_user("analyst")
    head, payload, sig = tok.split(".")
    tampered = f"{head}.{payload}.{sig[:-3]}abc"
    at = home(tampered)
    assert not has_token(at) and [t.key for t in at.text_input] == ["login_user", "login_pw"]


def test_a_deactivated_user_is_logged_out_on_the_next_interaction(live_api, ui_user):
    admin_name, admin_tok = ui_user("admin")
    name, tok = ui_user("analyst")
    assert has_token(home(tok))
    httpx.patch(f"{live_api}/admin/users/{name}", json={"is_active": False}, headers={"Authorization": f"Bearer {admin_tok}"})
    at = home(tok)
    assert not has_token(at) and "session expired" in texts(at)


def test_api_outage_at_login_gives_a_clear_message_and_recovery_works(live_api, users, monkeypatch):
    name, pw = users("analyst")
    monkeypatch.setenv("API_URL", "http://127.0.0.1:1")
    at = login(home(), name, pw)
    assert "Cannot reach the API" in texts(at) and not has_token(at) and not at.exception
    monkeypatch.setenv("API_URL", live_api)                                         # service restored: simply try again
    assert has_token(login(home(), name, pw))
