"""Deterministic question -> SQL templates over the gold tables. Used when no LLM is available (or the LLM's
SQL could not be made to pass the guard). Deliberately small: it answers common questions reliably and
returns None for everything else (the caller then says so instead of guessing)."""
from __future__ import annotations

import re
from dataclasses import dataclass

CHANNELS = ("web", "mobile", "store")
DEVICES = ("desktop", "mobile", "tablet")


@dataclass(frozen=True)
class TemplatePlan:
    name: str
    sql: str


def _top_n(q: str, default: int = 10) -> int:
    m = re.search(r"\b(?:top|best|highest|first)\s+(\d{1,3})\b", q) or re.search(r"\b(\d{1,3})\s+(?:top|best|highest)\b", q)
    return max(1, min(int(m.group(1)), 100)) if m else default


def _year(q: str) -> int | None:
    m = re.search(r"\b(20[12]\d)\b", q)
    return int(m.group(1)) if m else None


def _where(parts: list[str]) -> str:
    return (" WHERE " + " AND ".join(parts)) if parts else ""


def plan_from_template(question: str) -> TemplatePlan | None:
    q = re.sub(r"\s+", " ", question.lower())
    channel = next((c for c in CHANNELS if re.search(rf"\b{c}\b", q)), None)
    device = next((d for d in DEVICES if re.search(rf"\b{d}\b", q)), None)
    year = _year(q)
    n = _top_n(q)
    by_month = bool(re.search(r"\b(by|per) month\b|\bmonthly\b|\bmonth by month\b", q))
    by_channel = bool(re.search(r"\b(by|per|each) channel\b", q))
    by_device = bool(re.search(r"\b(by|per|each) device\b", q))
    by_carrier = bool(re.search(r"\b(by|per|each) carrier\b|\bcarriers?\b", q))
    sales_filters = ([f"channel = '{channel}'"] if channel else []) + ([f"EXTRACT(year FROM order_date) = {year}"] if year else [])

    if re.search(r"on[- ]?time", q):
        return TemplatePlan("on_time_by_carrier",
            "SELECT carrier, ROUND((SUM(on_time_pct * delivered) / NULLIF(SUM(delivered), 0))::numeric, 2) AS on_time_pct, "
            "SUM(delivered) AS delivered FROM gold.delivery_sla GROUP BY carrier ORDER BY on_time_pct DESC")
    if re.search(r"deliver\w* (time|speed)|lead time|how long.*deliver|average delivery", q):
        return TemplatePlan("lead_time_by_carrier",
            "SELECT carrier, ROUND((SUM(avg_lead_days * delivered) / NULLIF(SUM(delivered), 0))::numeric, 2) AS avg_lead_days, "
            "SUM(delivered) AS delivered FROM gold.delivery_sla GROUP BY carrier ORDER BY avg_lead_days")
    if re.search(r"conversion|purchase rate", q):
        return TemplatePlan("purchase_rate_by_device",
            "SELECT device, SUM(sessions) AS sessions, SUM(purchases) AS purchases, "
            "ROUND(SUM(purchases)::numeric / NULLIF(SUM(sessions), 0), 4) AS purchase_rate "
            "FROM gold.funnel_daily" + _where([f"device = '{device}'"] if device else []) +
            " GROUP BY device ORDER BY purchase_rate DESC")
    if re.search(r"funnel|page views?|product views?|add(ed)? to cart|checkout", q):
        return TemplatePlan("funnel_totals_by_device",
            "SELECT device, SUM(page_views) AS page_views, SUM(product_views) AS product_views, "
            "SUM(add_to_carts) AS add_to_carts, SUM(checkout_starts) AS checkout_starts, SUM(purchases) AS purchases "
            "FROM gold.funnel_daily" + _where([f"device = '{device}'"] if device else []) + " GROUP BY device ORDER BY device")
    if re.search(r"return(ed)? rate|returns? rate|share of returns", q):
        return TemplatePlan("return_rate_by_channel",
            "SELECT channel, SUM(orders) AS orders, SUM(returned_orders) AS returned_orders, "
            "ROUND(SUM(returned_orders)::numeric / NULLIF(SUM(orders), 0), 4) AS return_rate "
            "FROM gold.daily_sales" + _where(sales_filters) + " GROUP BY channel ORDER BY return_rate DESC")
    if re.search(r"customers?", q) and re.search(r"lifetime value|ltv|spend|top|best|valuable", q):
        return TemplatePlan("top_customers",
            "SELECT customer_id, segment, country, orders, lifetime_value FROM gold.customer_value "
            f"ORDER BY lifetime_value DESC LIMIT {n}")
    measure = "net_revenue" if re.search(r"net (revenue|sales)", q) else "gross_merchandise_value"
    label = "net_revenue" if measure == "net_revenue" else "gmv"
    if re.search(r"(best|top|highest|busiest|biggest) (\d+ )?days?", q):
        return TemplatePlan("top_days",
            f"SELECT order_date, SUM({measure}) AS {label}, SUM(orders) AS orders FROM gold.daily_sales"
            f"{_where(sales_filters)} GROUP BY order_date ORDER BY {label} DESC LIMIT {n}")
    wants_orders = re.search(r"\b(how many orders|number of orders|order count|orders)\b", q) and not \
        re.search(r"sales|revenue|gmv|merchandise", q)
    if wants_orders or re.search(r"sales|revenue|gmv|merchandise", q):
        agg, alias = ("SUM(orders)", "orders") if wants_orders else (f"SUM({measure})", label)
        if by_month:
            return TemplatePlan("by_month", f"SELECT DATE_TRUNC('month', order_date)::date AS month, {agg} AS {alias} "
                                f"FROM gold.daily_sales{_where(sales_filters)} GROUP BY 1 ORDER BY 1")
        if by_channel or (not channel and re.search(r"channels?", q)):
            return TemplatePlan("by_channel", f"SELECT channel, {agg} AS {alias} FROM gold.daily_sales"
                                f"{_where(sales_filters[1:] if channel else sales_filters)} GROUP BY channel ORDER BY {alias} DESC")
        return TemplatePlan("total", f"SELECT {agg} AS {alias} FROM gold.daily_sales{_where(sales_filters)}")
    return None
