    SELECT 
    OS.STOREID,
    SUM(OFACT.REVENUE) AS ActualSales,
    SUM(ST.SALESTARGET) AS TargetSales,
FROM 
    {{ ref('orders_stg') }} OS
JOIN 
    {{ ref('orders_fact') }} OFACT ON OS.ORDERID = OFACT.OrderID
JOIN
    {{ ref('salestarget') }} ST ON ST.StoreID = OS.StoreID
GROUP BY 1