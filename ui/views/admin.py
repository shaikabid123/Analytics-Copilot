import streamlit as st

import api_client as ac

ac.require_login()
st.title("Users & audit")
if st.session_state.get("flash"):
    kind, text = st.session_state.pop("flash")
    (st.success if kind == "success" else st.error)(text)
tab_users, tab_audit, tab_sql = st.tabs(["Users", "Audit events", "SQL audit"])

with tab_users:
    users = ac.need(ac.api().get("/admin/users"))
    st.dataframe(ac.df(["username", "role", "active", "locked"], [[u["username"], u["role"], u["is_active"], u["locked"]] for u in users]), width="stretch", hide_index=True)
    with st.form("new_user"):
        st.markdown("**Create user**")
        n = st.text_input("Username", key="nu_name", max_chars=32)
        p = st.text_input("Password (12+ characters)", type="password", key="nu_pw", max_chars=128)
        r = st.selectbox("Role", ["analyst", "engineer", "admin"], key="nu_role")
        if st.form_submit_button("Create"):
            res = ac.api().post("/admin/users", {"username": n.strip(), "password": p, "role": r})
            st.session_state["flash"] = ("success", f"Created {n} ({r}).") if res.ok else ("error", res.error)
            st.rerun()
    pick = st.selectbox("Manage user", [u["username"] for u in users], key="mu_pick")
    c1, c2, c3 = st.columns(3)
    new_role = c1.selectbox("Set role", ["analyst", "engineer", "admin"], key="mu_role")
    if c1.button("Apply role", key="mu_apply"):
        res = ac.api().patch(f"/admin/users/{pick}", {"role": new_role})
        st.session_state["flash"] = ("success", f"{pick} is now {new_role}.") if res.ok else ("error", res.error)
        st.rerun()
    for col, label, val in ((c2, "Deactivate", False), (c3, "Reactivate", True)):
        if col.button(label, key=f"mu_{label.lower()}"):
            res = ac.api().patch(f"/admin/users/{pick}", {"is_active": val})
            st.session_state["flash"] = ("success", f"{label}d {pick}.") if res.ok else ("error", res.error)
            st.rerun()

with tab_audit:
    action = st.text_input("Filter by action (exact)", key="au_action", max_chars=64, placeholder="e.g. authz.denied")
    ev = ac.need(ac.api().get("/audit/events", limit=100, action=action.strip() or None))
    st.dataframe(ac.df(["at", "actor", "action", "outcome", "resource"], [[e["at"], e["actor"], e["action"], e["outcome"], e["resource"]] for e in ev]), width="stretch", hide_index=True)

with tab_sql:
    qa = ac.need(ac.api().get("/audit/queries", limit=100))
    st.dataframe(ac.df(["at", "actor", "role", "status", "source", "sql", "reasons"], [[q["at"], q["actor"], q["role"], q["status"], q["source"], q["sql"][:120], "; ".join(q["reasons"] or [])] for q in qa]),
                 width="stretch", hide_index=True)
