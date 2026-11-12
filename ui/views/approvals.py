import streamlit as st

import api_client as ac

ac.require_login()
st.title("Approval queue")
st.caption("Changes proposed by the agents wait here. You cannot decide a request that you triggered yourself.")
if st.session_state.get("flash"):
    kind, text = st.session_state.pop("flash")
    (st.success if kind == "success" else st.error)(text)

status = st.radio("Show", ["pending", "approved", "rejected", "all"], horizontal=True, key="ap_status")
items = ac.need(ac.api().get("/approvals", status=status, limit=100))
if not items:
    st.info(f"No {status if status != 'all' else ''} approvals.")
for a in items:
    with st.expander(f"#{a['id']} · {a['status']} · {a['summary'][:90]}", expanded=a["status"] == "pending"):
        st.write(a["summary"])
        st.caption(f"Requested by **{a['requested_by']}** at {a['created_at']}" +
                   (f" · decided by **{a['decided_by']}** at {a['decided_at']}" if a["decided_by"] else ""))
        p = a["proposal"] or {}
        st.markdown(f"**Proposed action:** `{p.get('action')}` {p.get('params') or ''} on pipeline `{p.get('pipeline')}`")
        st.markdown(f"**Why:** {p.get('rationale')}")
        st.warning(f"Risk: {p.get('risk')}")
        if a["note"]:
            st.caption(f"Decision note: {a['note']}")
        if a["applied_at"]:
            st.success(f"Applied at {a['applied_at']}")
        if a["status"] == "pending":
            note = st.text_input("Note (optional)", key=f"note_{a['id']}", max_chars=500)
            c1, c2 = st.columns(2)
            for col, label, decision in ((c1, "Approve", "approved"), (c2, "Reject", "rejected")):
                if col.button(label, key=f"{decision}_{a['id']}"):
                    res = ac.api().post(f"/approvals/{a['id']}/decision", {"decision": decision, "note": note or None})
                    st.session_state["flash"] = (("success", f"Approval #{a['id']} {decision}. {res.data['answer'] or ''}") if res.ok else ("error", res.error))
                    st.rerun()
