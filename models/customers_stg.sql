{{ config(materialized='ephemeral')}}
    SELECT
    CustomerID,
    FirstName,
    LastName,
    Email,
    Phone,
    Address,
    city,
    State,
    ZipCode,
    Updated_at,
    CONCAT (FirstName, '', LastName) AS CustomerName
FROM
    {{source('default', 'cust')}}