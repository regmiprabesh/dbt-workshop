    {{ config(materialized='incremental', unique_key='ORDERID')}}
    SELECT
    OrderID,
    OrderDate, 
    CustomerID, 
    EmployeeID, 
    StoreID,
    Status AS StatusCD,
    CASE
        WHEN Status = '01' THEN 'In Progress'
        WHEN Status = '02' THEN 'Completed'
        WHEN Status = '03' THEN 'Cancelled'
    END AS StatusDesc, Updated_at
    Updated_at
FROM
    {{source('default', 'ordr')}}

{% if is_incremental() %}
where Updated_at >= (select max(dbt_updated_at) from {{ this }})
{% endif %}