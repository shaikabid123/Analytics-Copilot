import streamlit as st

import api_client as ac

ac.require_login()
st.title("Data catalog & glossary")
st.caption("Metadata only: no row data is shown here.")
tab_search, tab_browse, tab_gloss = st.tabs(["Search", "Browse datasets", "Glossary"])

with tab_search:
    q = st.text_input("Search tables, columns and terms", key="catalog_q", max_chars=100, placeholder="e.g. customer email, margin, delivery")
    if q and len(q.strip()) < 2:
        st.warning("Type at least 2 characters.")
    elif q:
        hits = ac.need(ac.api().get("/catalog/search", q=q.strip(), limit=30))
        if not hits:
            st.info("No matches. Try a shorter or different word.")
        else:
            st.write(f"{len(hits)} result(s)")
            st.dataframe(ac.df(["kind", "name", "snippet", "rank"], [[h["kind"], h["name"], h["snippet"], round(h["rank"], 3)] for h in hits]),
                         width="stretch", hide_index=True)

with tab_browse:
    sources = ["(all)"] + [s["name"] for s in ac.need(ac.api().get("/sources"))]
    c1, c2 = st.columns(2)
    source = c1.selectbox("Source", sources, key="catalog_source")
    layer = c2.selectbox("Layer", ["(all)", "source", "bronze", "silver", "gold"], key="catalog_layer")
    datasets = ac.need(ac.api().get("/catalog/datasets", source=None if source == "(all)" else source,
                                    layer=None if layer == "(all)" else layer, limit=200))
    if not datasets:
        st.info("No datasets match these filters.")
    else:
        table = ac.df(["source", "dataset", "layer", "owner", "rows", "description"],
                      [[d["source"], f"{d['schema_name']}.{d['table_name']}", d["layer"], d["owner_name"], d["row_count"], d["description"]] for d in datasets])
        st.dataframe(table, width="stretch", hide_index=True)
        st.download_button("Download list as CSV", table.to_csv(index=False), "catalog_datasets.csv", "text/csv", key="catalog_csv")
        pick = st.selectbox("Dataset details", datasets, key="catalog_pick", format_func=lambda d: f"{d['source']} / {d['schema_name']}.{d['table_name']}")
        detail = ac.need(ac.api().get(f"/catalog/datasets/{pick['id']}"))
        st.write(detail["description"] or "_No description yet._")
        st.dataframe(ac.df(["column", "type", "nullable", "PII", "description"],
                           [[c["name"], c["data_type"], c["is_nullable"], "PII" if c["is_pii"] else "", c["description"]] for c in detail["columns"]]),
                     width="stretch", hide_index=True)

with tab_gloss:
    gq = st.text_input("Filter glossary", key="gloss_q", max_chars=100)
    terms = ac.need(ac.api().get("/glossary", q=gq.strip() if gq and len(gq.strip()) >= 2 else None))
    if not terms:
        st.info("No glossary terms found.")
    for t in terms:
        with st.expander(t["term"]):
            st.write(t["definition"])
            linked = ac.need(ac.api().get(f"/glossary/{t['id']}"), stop=False)
            if linked and linked["columns"]:
                st.caption("Implemented by: " + ", ".join(f"`{c['dataset']}.{c['column']}`" for c in linked["columns"]))
