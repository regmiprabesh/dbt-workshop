   SELECT
    OrderItemID, 
    orderID, 
    ProductID, 
    Quantity, 
    UnitPrice,
    Quantity * UnitPrice AS TotalPrice,
    Updated_at
FROM
    {{source('default', 'ordritms')}}