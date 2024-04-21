
SELECT OrderID
FROM {{ ref('orders_fact') }}
WHERE REVENUE < 0