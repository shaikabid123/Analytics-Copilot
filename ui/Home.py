"""Entry point: login gate + role-based navigation. Navigation only HIDES pages for convenience; every page calls the
secured API, which enforces roles itself, so a hidden page that is reached anyway still gets 403 from the backend."""
import streamlit as st

import api_client as ac

st.set_page_config(page_title="Data Engineering Copilot", page_icon="🗄️", layout="wide")


def login_screen() -> None:
    st.title("AI Data Engineering & Analytics Copilot")
    if st.session_state.get("notice"):
        st.warning(st.session_state.pop("notice"))
    with st.form("login"):
        user = st.text_input("Username", key="login_user", max_chars=64)
        pw = st.text_input("Password", type="password", key="login_pw", max_chars=256)
        go = st.form_submit_button("Log in")
    if go:
        if not user or not pw:
            st.error("Enter both username and password.")
            return
        res = ac.Api().login(user.strip(), pw)
        if res.ok:
            st.session_state["token"] = res.data["access_token"]
            st.session_state.pop("login_pw", None)
            st.rerun()
        elif res.kind == "unauthenticated" or res.status == 401:
            st.error("Invalid username or password (repeated failures lock the account for 15 minutes).")
        else:
            ac.render_error(res)
    st.caption(f"API: {ac.base_url()}")


def sidebar(me: dict) -> None:
    with st.sidebar:
        st.markdown(f"**{me['username']}** · role: `{me['role']}`")
        health = ac.Api().get("/health")
        st.caption("API: up" if health.ok else "API: unreachable")
        if st.button("Log out", key="logout"):
            ac.logout()
            st.rerun()


def pages_for(role: str) -> dict:
    P = lambda f, title, icon: st.Page(f"views/{f}.py", title=title, icon=icon)   # noqa: E731
    nav = {"Explore": [P("catalog", "Catalog & glossary", "📚"), P("sources", "Data sources", "🔌"),
                       P("query", "Ask & SQL", "💬"), P("analytics", "Gold analytics", "📊")]}
    if role in ("engineer", "admin"):
        nav["Operate"] = [P("pipelines", "Pipelines", "⚙️"), P("quality", "Quality & drift", "🧪"),
                          P("approvals", "Approval queue", "✅"), P("monitoring", "Monitoring", "📈")]
    if role == "admin":
        nav["Administer"] = [P("admin", "Users & audit", "🛡️")]
    return nav


token = st.session_state.get("token")
if not token:
    login_screen()
else:
    res = ac.api().get("/auth/me")                       # the backend is asked who we are on every run (expiry, role changes)
    if not res.ok:
        if res.kind == "unauthenticated":
            ac.logout("Your session expired. Please log in again.")
            st.rerun()
        ac.render_error(res)
        st.stop()
    me = res.data
    st.session_state["username"], st.session_state["role"] = me["username"], me["role"]   # display only: never used to authorize
    sidebar(me)
    st.navigation(pages_for(me["role"])).run()
