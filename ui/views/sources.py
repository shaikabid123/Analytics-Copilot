import streamlit as st

import api_client as ac

ac.require_login()
st.title("Data sources")
sources = ac.need(ac.api().get("/sources"))
datasets = ac.need(ac.api().get("/catalog/datasets", limit=200))
by_source: dict[str, list] = {}
for d in datasets:
    by_source.setdefault(d["source"], []).append(d)
rows = []
for s in sources:
    ds = by_source.get(s["name"], [])
    synced = max((d["last_synced_at"] for d in ds if d["last_synced_at"]), default=None)
    rows.append([s["name"], s["source_type"], len(ds), sum(d["row_count"] or 0 for d in ds), synced, s["description"]])
st.dataframe(ac.df(["source", "type", "datasets", "rows", "last synced", "description"], rows), width="stretch", hide_index=True)
if not sources:
    st.info("No sources are registered yet. An operator registers them with the CLI (`app.cli register-sources`).")
pick = st.selectbox("Inspect a source", [s["name"] for s in sources], key="source_pick") if sources else None
if pick:
    st.dataframe(ac.df(["dataset", "layer", "owner", "rows", "last synced"],
                       [[f"{d['schema_name']}.{d['table_name']}", d["layer"], d["owner_name"], d["row_count"], d["last_synced_at"]]
                        for d in by_source.get(pick, [])]), width="stretch", hide_index=True)
st.caption("Connection settings and credentials are never shown here; they are managed by operators outside the UI.")
