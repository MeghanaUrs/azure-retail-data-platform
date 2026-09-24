-- Synapse Serverless SQL Pool: external tables + views over the Gold Delta layer.
-- Serverless (not dedicated) pool chosen since this is a reporting workload with
-- moderate query volume — pay-per-query is more cost-effective than provisioned
-- compute at this scale.

-- 1. Data source pointing at the Gold zone of the lake
CREATE EXTERNAL DATA SOURCE gold_lake
WITH (LOCATION = 'abfss://gold@retaildataplatformadls.dfs.core.windows.net');
GO

-- 2. External tables over each Gold Delta table
CREATE EXTERNAL TABLE ext_fct_sales (
    order_id        VARCHAR(20),
    customer_key    VARCHAR(20),
    product_key     VARCHAR(20),
    date_key        DATE,
    quantity        INT,
    unit_price      DECIMAL(10,2),
    net_amount      DECIMAL(10,2),
    ship_province   VARCHAR(5),
    order_status    VARCHAR(20)
)
WITH (
    LOCATION = 'fct_sales',
    DATA_SOURCE = gold_lake,
    FILE_FORMAT = DeltaFormat
);
GO

CREATE EXTERNAL TABLE ext_dim_customer (
    customer_key    VARCHAR(20),
    customer_name   VARCHAR(100),
    segment         VARCHAR(20),
    province        VARCHAR(5),
    signup_date     DATE,
    email_domain    VARCHAR(50)
)
WITH (LOCATION = 'dim_customer', DATA_SOURCE = gold_lake, FILE_FORMAT = DeltaFormat);
GO

CREATE EXTERNAL TABLE ext_dim_product (
    product_key     VARCHAR(20),
    product_name    VARCHAR(100),
    category        VARCHAR(50),
    cost            DECIMAL(10,2),
    list_price      DECIMAL(10,2),
    margin_pct      DECIMAL(5,1),
    supplier        VARCHAR(50)
)
WITH (LOCATION = 'dim_product', DATA_SOURCE = gold_lake, FILE_FORMAT = DeltaFormat);
GO

-- 3. Reporting view: this is what Power BI connects to directly.
-- Kept as a single denormalized view (rather than making Power BI join
-- three tables at query time) to keep import/DirectQuery performance predictable.
CREATE OR ALTER VIEW vw_sales_reporting AS
SELECT
    f.order_id,
    f.date_key,
    c.customer_name,
    c.segment          AS customer_segment,
    c.province          AS customer_province,
    p.product_name,
    p.category,
    p.supplier,
    f.quantity,
    f.unit_price,
    f.net_amount,
    p.list_price - p.cost                          AS unit_margin,
    (p.list_price - p.cost) * f.quantity            AS line_margin,
    f.ship_province,
    f.order_status
FROM ext_fct_sales f
JOIN ext_dim_customer c ON f.customer_key = c.customer_key
JOIN ext_dim_product  p ON f.product_key  = p.product_key;
GO

-- 4. Fulfillment metric view, separate from sales revenue view since it
-- answers a different question (operational, not financial).
CREATE OR ALTER VIEW vw_fulfillment_summary AS
SELECT
    ship_province,
    order_status,
    COUNT(*)            AS order_count,
    SUM(quantity)        AS units
FROM ext_fct_sales
GROUP BY ship_province, order_status;
GO
