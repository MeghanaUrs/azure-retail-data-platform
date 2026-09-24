# Power BI reporting layer

**Connection:** DirectQuery to `vw_sales_reporting` and `vw_fulfillment_summary` in Synapse serverless SQL — no data duplicated into Power BI's own storage, so the report always reflects the latest Gold refresh.

## Report pages

1. **Sales Overview** — total revenue, units sold, and order count by month, with category and province slicers. Revenue trend line, category breakdown bar chart.
2. **Customer Segment View** — revenue and order count split by customer segment (Retail vs Wholesale) and province, built directly off `vw_sales_reporting` columns.
3. **Fulfillment Summary** — order status mix (Delivered / Cancelled / Returned) by province, sourced from `vw_fulfillment_summary`, to flag regions with unusual return/cancellation rates.

## Measures

Kept intentionally simple — most shaping already happened upstream in Gold and the Synapse view, so Power BI's job is presentation, not heavy calculation:

```DAX
Total Revenue = SUM(vw_sales_reporting[net_amount])
Total Units = SUM(vw_sales_reporting[quantity])
Order Count = DISTINCTCOUNT(vw_sales_reporting[order_id])
Total Margin = SUM(vw_sales_reporting[line_margin])
```

No time-intelligence DAX (YoY, running totals, etc.) is used in this version — that's a deliberate scope line, not an oversight: the aggregation and date logic live in the Gold layer (`agg_sales_by_category_month`, `dim_date`) where I have direct hands-on depth, and the report layer stays thin on top of it.

## Refresh

Scheduled refresh not required in DirectQuery mode against Synapse — the report reflects each Gold-layer refresh as soon as the ADF pipeline completes.
