# Data dictionary — Gold layer

## fct_sales (grain: one row per order)

| Column | Type | Notes |
|---|---|---|
| order_id | string | Natural key |
| customer_key | string | FK → dim_customer |
| product_key | string | FK → dim_product |
| date_key | date | FK → dim_date |
| quantity | int | |
| unit_price | decimal(10,2) | |
| net_amount | decimal(10,2) | quantity × unit_price |
| ship_province | string | 2-letter province code, uppercased in Silver |
| order_status | string | Delivered / Returned / Cancelled orders are excluded upstream in Silver |

## dim_customer

| Column | Type | Notes |
|---|---|---|
| customer_key | string | PK |
| customer_name | string | |
| segment | string | Retail / Wholesale |
| province | string | |
| signup_date | date | |
| email_domain | string | |

## dim_product

| Column | Type | Notes |
|---|---|---|
| product_key | string | PK |
| product_name | string | |
| category | string | |
| cost | decimal(10,2) | |
| list_price | decimal(10,2) | |
| margin_pct | decimal(5,1) | (list_price − cost) / list_price × 100, computed in Silver |
| supplier | string | |

## dim_date

| Column | Type | Notes |
|---|---|---|
| date_key | date | PK |
| year | int | |
| month | int | |
| day_of_week | string | |
| is_weekend | boolean | |

## agg_sales_by_category_month (pre-aggregated)

| Column | Type | Notes |
|---|---|---|
| order_month | date | first of month |
| category | string | |
| ship_province | string | |
| total_revenue | decimal | |
| total_units | int | |
| order_count | int | distinct order_id count |
