{% macro generate_profit_model(table_name) %}
SELECT
sales_date,
SUM (quantity_sold * unit_sell_price) as total_revenue,
SUM(quantity_sold * unit_purchase_cost) as total_cost,
SUM(quantity_sold * unit_sell_price) - SUM(quantity_sold * unit_purchase_cost) as total_profit
FROM {{ source('default', table_name) }}
GROUP BY sales_date
 {% endmacro %}