import pandas as pd
import streamlit as st

import api_client as ac

ac.require_login()
st.title("Gold analytics")
st.caption("Every chart runs a fixed read-only query through the same secured SQL endpoint as the SQL console.")
CHANNELS = ("all", "web", "mobile", "store")


def run(sql: str, key: str):
    res = ac.api().post("/query/execute", {"sql": sql})
    if not res.ok:
        ac.render_error(res)
        return None
    table = ac.df(res.data["columns"], res.data["rows"])
    with st.expander("SQL"):
        st.code(res.data["sql"], language="sql")
    st.download_button("Download CSV", table.to_csv(index=False), f"{key}.csv", "text/csv", key=f"csv_{key}")
    return table


board = st.selectbox("Dashboard", ["Sales trend", "Channels", "Web funnel", "Delivery SLA", "Top customers"], key="board")
if board == "Sales trend":
    ch = st.selectbox("Channel", CHANNELS, key="board_channel")
    assert ch in CHANNELS                                     # only fixed values are ever placed in the query
    where = "" if ch == "all" else f" WHERE channel = '{ch}'"
    t = run("SELECT DATE_TRUNC('month', order_date)::date AS month, SUM(gross_merchandise_value) AS gmv, SUM(net_revenue) AS net_revenue "
            f"FROM gold.daily_sales{where} GROUP BY 1 ORDER BY 1", "sales_trend")
    if t is not None and len(t):
        t["month"] = pd.to_datetime(t["month"])
        st.line_chart(t.set_index("month")[["gmv", "net_revenue"]])
        st.metric("Total GMV", f"{t['gmv'].sum():,.0f}")
elif board == "Channels":
    t = run("SELECT channel, SUM(orders) AS orders, SUM(gross_merchandise_value) AS gmv, "
            "ROUND(SUM(returned_orders)::numeric / NULLIF(SUM(orders), 0), 4) AS return_rate "
            "FROM gold.daily_sales GROUP BY channel ORDER BY gmv DESC", "channels")
    if t is not None and len(t):
        st.bar_chart(t.set_index("channel")["gmv"])
        st.dataframe(t, width="stretch", hide_index=True)
elif board == "Web funnel":
    t = run("SELECT device, SUM(sessions) AS sessions, SUM(page_views) AS page_views, SUM(product_views) AS product_views, "
            "SUM(add_to_carts) AS add_to_carts, SUM(checkout_starts) AS checkout_starts, SUM(purchases) AS purchases, "
            "ROUND(SUM(purchases)::numeric / NULLIF(SUM(sessions), 0), 4) AS purchase_rate FROM gold.funnel_daily GROUP BY device ORDER BY device", "funnel")
    if t is not None and len(t):
        st.bar_chart(t.set_index("device")[["page_views", "product_views", "add_to_carts", "checkout_starts", "purchases"]])
        st.dataframe(t, width="stretch", hide_index=True)
elif board == "Delivery SLA":
    t = run("SELECT carrier, ROUND((SUM(avg_lead_days * delivered) / NULLIF(SUM(delivered), 0))::numeric, 2) AS avg_lead_days, "
            "ROUND((SUM(on_time_pct * delivered) / NULLIF(SUM(delivered), 0))::numeric, 2) AS on_time_pct, SUM(delivered) AS delivered "
            "FROM gold.delivery_sla GROUP BY carrier ORDER BY on_time_pct DESC", "delivery_sla")
    if t is not None and len(t):
        st.bar_chart(t.set_index("carrier")["on_time_pct"])
        st.dataframe(t, width="stretch", hide_index=True)
else:
    st.caption("Customer-level data: engineers and admins only. Analysts are refused by the server.")
    t = run("SELECT customer_id, segment, country, orders, lifetime_value FROM gold.customer_value "
            "ORDER BY lifetime_value DESC LIMIT 20", "top_customers")
    if t is not None:
        st.dataframe(t, width="stretch", hide_index=True)
