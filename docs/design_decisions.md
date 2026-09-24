# Design decisions

**Batch over streaming.** Source systems deliver daily file extracts, not events — there's no real-time requirement in this scenario. Building a streaming pipeline (Event Hubs + Structured Streaming) here would be over-engineering for the stated problem, so ADF scheduled triggers + daily Databricks jobs were used instead. This also matches where my hands-on production experience actually is: batch ETL/ELT.

**Medallion (Bronze/Silver/Gold) over a single-hop pipeline.** Keeping Bronze immutable and untransformed means any bug discovered in Silver or Gold logic can be fixed and replayed without re-pulling from source systems — important in a scenario with third-party source feeds you don't control the retention of.

**Serverless Synapse over dedicated SQL pool.** At this data volume, a dedicated pool's provisioned compute would sit mostly idle. Serverless bills per query scanned, which fits a reporting workload with predictable, moderate query volume.

**Excluding cancelled orders from the fact grain in Silver, not Gold.** Whether to include cancelled orders is a business-rule decision, not a presentation-layer one — so it happens once, early, in Silver, rather than being re-implemented (and potentially inconsistently) in every downstream Gold table or Power BI measure.

**Pre-aggregated `agg_sales_by_category_month` table.** Rather than have Power BI aggregate the full fact table on every query, a monthly-grain aggregate is materialized in Gold. This is a common pattern for keeping DirectQuery reports responsive without needing Power BI's own import cache.

**Scope lines deliberately not crossed in this version:**
- No slowly changing dimension (SCD Type 2) handling on `dim_customer`/`dim_product` — customer/product attributes are treated as current-state-only for this version. A production version handling historical attribute changes would add `effective_date`/`end_date` and a surrogate key.
- No CI/CD pipeline (Databricks Asset Bundles / Azure DevOps) wired up yet — pipeline and notebook code is written to be deployable as-is, but the deployment automation itself is a natural next iteration.
- Power BI report is intentionally light on DAX (see `powerbi/README.md`) — reflects genuine current depth rather than padding the stack.
