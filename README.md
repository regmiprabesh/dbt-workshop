Welcome to your DBT(Data Build Tool) Workshop! This repository contains the project that you will be making throughout the process below.

### Prerequisties
- Python
- Snowflake Trial Account

### Steps 
- Check python installation
    > python --version
- Create python virtual environment
    > python -m venv dbt_venv
- Activate python virtual environment
1. For windows
    >dbt_venv\Scripts\activate
2. For Mac/Linux
    > source dbt_venv/bin/activate
- Install dbt package with python for snowflake
    > python install dbt-snowflake
-   Check dbt installation
    > dbt --version
-   Initialize dbt project
    > dbt init

    Add necessary credentials asked using your snowflake account when asked in the terminal

    Create warehouse in snowflake using
    > Create warehouse dbt_warehouse with warehouse_size = xsmall

    For dummy snowflake table run the following query in you snowflake worksheet

    ``` sql
    CREATE DATABASE DBT_WORKSHOP;

    CREATE SCHEMA L1_DEFAULT;

    CREATE SCHEMA L2_PROCESSING;

    CREATE SCHEMA L3_CONSUMPTION;
    
    USE database DBT_WORKSHOP;

    USE SCHEMA L1_DEFAULT;

    CREATE TABLE IF NOT EXISTS Dates (
    Date DATE NOT NULL,
    Day VARCHAR(3) NULL,
    Month VARCHAR(10) NULL,
    Year VARCHAR(4) NULL,
    Quarter INT NULL,
    DayOfWeek VARCHAR(10) NULL,
    WeekOfYear INT NULL,
    Updated_at TIMESTAMP NULL,
    PRIMARY KEY (Date));
    
    CREATE TABLE IF NOT EXISTS customers (
    CustomerID INT NOT NULL,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    Phone VARCHAR(100),
    Address VARCHAR(100),
    City VARCHAR(50),
    State VARCHAR(2),
    ZipCode VARCHAR(10),
    Updated_at TIMESTAMP,
    PRIMARY KEY (CustomerID));
    
    CREATE TABLE IF NOT EXISTS Employees (
    EmployeeID INT NOT NULL,
    FirstName VARCHAR(100) NULL,
    LastName VARCHAR(100) NULL,
    Email VARCHAR(200) NULL,
    JobTitle VARCHAR(100) NULL,
    HireDate DATE NULL,
    ManagerID INT NULL,
    Address VARCHAR(200) NULL,
    City VARCHAR(50) NULL,
    State VARCHAR(50) NULL,
    ZipCode VARCHAR(10) NULL,
    Updated_at TIMESTAMP NULL,
    PRIMARY KEY (EmployeeID));
    
    CREATE TABLE IF NOT EXISTS Stores (
    StoreID INT NOT NULL,
    StoreName VARCHAR(100) NULL,
    Address VARCHAR(200) NULL,
    City VARCHAR(50) NULL,
    State VARCHAR(50) NULL,
    ZipCode VARCHAR(10) NULL,
    Email VARCHAR(200) NULL,
    Phone VARCHAR(50) NULL,
    Updated_at TIMESTAMP NULL,
    PRIMARY KEY (StoreID));
    
    CREATE TABLE IF NOT EXISTS Suppliers (
    SupplierID INT NOT NULL,
    SupplierName VARCHAR(100) NULL,
    ContactPerson VARCHAR(100) NULL,
    Email VARCHAR(200) NULL,
    Phone VARCHAR(50) NULL,
    Address VARCHAR(50) NULL,
    City VARCHAR(50) NULL,
    State VARCHAR(10) NULL,
    ZipCode VARCHAR(10) NULL,
    Updated_at TIMESTAMP NULL,
    PRIMARY KEY (SupplierID));
    
    CREATE TABLE IF NOT EXISTS Products (
    ProductID INT NOT NULL,
    Name VARCHAR(100) NULL,
    Category VARCHAR(100) NULL,
    RetailPrice DECIMAL(10,2) NULL,
    SupplierPrice DECIMAL(10,2) NULL,
    SupplierID INT NULL,
    Updated_at TIMESTAMP NULL,
    PRIMARY KEY (ProductID));
    
    CREATE TABLE IF NOT EXISTS OrderItems (
    OrderItemID INT NOT NULL,
    OrderID INT NULL,
    ProductID INT NULL,
    Quantity INT NULL,
    UnitPrice DECIMAL(10,2) NULL,
    Updated_at TIMESTAMP NULL,
    PRIMARY KEY (OrderItemID));
    
    CREATE TABLE IF NOT EXISTS Orders (
    OrderID INT NOT NULL,
    OrderDate DATE NULL,
    CustomerID INT NULL,
    EmployeeID INT NULL,
    StoreID INT NULL,
    Status INT NULL,
    Updated_at TIMESTAMP NULL,
    PRIMARY KEY (OrderID));
    ```
    For inserting dummy snowflake data  run the following query in you snowflake worksheet

    ``` sql

    INSERT INTO Dates (Date, Day, Month, Year, Quarter, DayOfWeek, WeekOfYear, Updated_at)
    VALUES ('2024-01-01', 'Tue', 'January', '2024', 1, 'Tuesday', 1, CURRENT_TIMESTAMP),
           ('2024-01-02', 'Wed', 'January', '2024', 1, 'Wednesday', 1, CURRENT_TIMESTAMP),
           ('2024-01-03', 'Thu', 'January', '2024', 1, 'Thursday', 1, CURRENT_TIMESTAMP),
           ('2024-01-04', 'Fri', 'January', '2024', 1, 'Friday', 1, CURRENT_TIMESTAMP),
           ('2024-01-05', 'Sat', 'January', '2024', 1, 'Saturday', 1, CURRENT_TIMESTAMP),
           ('2024-01-06', 'Sun', 'January', '2024', 1, 'Sunday', 1, CURRENT_TIMESTAMP);
    
    INSERT INTO customers (CustomerID, FirstName, LastName, Email, Phone, Address, City, State, ZipCode, Updated_at)
    VALUES (1, 'John', 'Doe', 'john.doe@example.com', '1234567890', '123 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (2, 'Jane', 'Doe', 'jane.doe@example.com', '0987654321', '456 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (3, 'Jim', 'Smith', 'jim.smith@example.com', '1112223333', '789 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (4, 'Jill', 'Smith', 'jill.smith@example.com', '9998887777', '321 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (5, 'Joe', 'Bloggs', 'joe.bloggs@example.com', '5554443333', '654 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP);
    
    INSERT INTO Employees (EmployeeID, FirstName, LastName, Email, JobTitle, HireDate, ManagerID, Address, City, State, ZipCode, Updated_at)
    VALUES (1, 'Alice', 'Johnson', 'alice.johnson@example.com', 'Manager', '2023-01-01', NULL, '123 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (2, 'Bob', 'Smith', 'bob.smith@example.com', 'Sales Associate', '2023-02-01', 1, '456 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (3, 'Charlie', 'Brown', 'charlie.brown@example.com', 'Cashier', '2023-03-01', 1, '789 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (4, 'David', 'Davis', 'david.davis@example.com', 'Stock Clerk', '2023-04-01', 1, '321 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (5, 'Eve', 'Evans', 'eve.evans@example.com', 'Sales Associate', '2023-05-01', 1, '654 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP);
    
    INSERT INTO Stores (StoreID, StoreName, Address, City, State, ZipCode, Email, Phone, Updated_at)
    VALUES (1, 'Store 1', '123 Main St', 'Anytown', 'AS', '12345', 'store1@example.com', '1234567890', CURRENT_TIMESTAMP),
           (2, 'Store 2', '456 Main St', 'Anytown', 'AS', '12345', 'store2@example.com', '0987654321', CURRENT_TIMESTAMP),
           (3, 'Store 3', '789 Main St', 'Anytown', 'AS', '12345', 'store3@example.com', '1112223333', CURRENT_TIMESTAMP),
           (4, 'Store 4', '321 Main St', 'Anytown', 'AS', '12345', 'store4@example.com', '9998887777', CURRENT_TIMESTAMP),
           (5, 'Store 5', '654 Main St', 'Anytown', 'AS', '12345', 'store5@example.com', '5554443333', CURRENT_TIMESTAMP);
    
    INSERT INTO Suppliers (SupplierID, SupplierName, ContactPerson, Email, Phone, Address, City, State, ZipCode, Updated_at)
    VALUES (1, 'Supplier 1', 'John Doe', 'supplier1@example.com', '1234567890', '123 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (2, 'Supplier 2', 'Jane Doe', 'supplier2@example.com', '0987654321', '456 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (3, 'Supplier 3', 'Jim Smith', 'supplier3@example.com', '1112223333', '789 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (4, 'Supplier 4', 'Jill Smith', 'supplier4@example.com', '9998887777', '321 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP),
           (5, 'Supplier 5', 'Joe Bloggs', 'supplier5@example.com', '5554443333', '654 Main St', 'Anytown', 'AS', '12345', CURRENT_TIMESTAMP);
    
    INSERT INTO Products (ProductID, Name, Category, RetailPrice, SupplierPrice, SupplierID, Updated_at)
    VALUES (1, 'Product 1', 'Category 1', 9.99, 4.99, 1, CURRENT_TIMESTAMP),
           (2, 'Product 2', 'Category 1', 19.99, 9.99, 1, CURRENT_TIMESTAMP),
           (3, 'Product 3', 'Category 2', 29.99, 14.99, 2, CURRENT_TIMESTAMP),
           (4, 'Product 4', 'Category 2', 39.99, 19.99, 2, CURRENT_TIMESTAMP),
           (5, 'Product 5', 'Category 3', 49.99, 24.99, 3, CURRENT_TIMESTAMP);
    
    INSERT INTO OrderItems (OrderItemID, OrderID, ProductID, Quantity, UnitPrice, Updated_at)
    VALUES (1, 1, 1, 1, 9.99, CURRENT_TIMESTAMP),
       (2, 1, 2, 2, 19.99, CURRENT_TIMESTAMP),
       (3, 2, 3, 3, 29.99, CURRENT_TIMESTAMP),
       (4, 2, 4, 4, 39.99, CURRENT_TIMESTAMP),
       (5, 2, 4, 4, 39.99, CURRENT_TIMESTAMP),
       (6, 5, 4, 4, 39.99, CURRENT_TIMESTAMP),
       (7, 5, 4, 4, 39.99, CURRENT_TIMESTAMP),
       (8, 3, 5, 5, 49.99, CURRENT_TIMESTAMP),
       (9, 1, 1, 1, 9.99, CURRENT_TIMESTAMP),
       (10, 6, 2, 2, 19.99, CURRENT_TIMESTAMP),
       (11, 8, 3, 3, 29.99, CURRENT_TIMESTAMP),
       (12, 9, 4, 4, 39.99, CURRENT_TIMESTAMP),
       (13, 10, 4, 4, 39.99, CURRENT_TIMESTAMP),
       (14, 11, 4, 4, 39.99, CURRENT_TIMESTAMP),
       (15, 7, 4, 4, 39.99, CURRENT_TIMESTAMP),
       (16, 9, 5, 5, 49.99, CURRENT_TIMESTAMP);

    
    INSERT INTO Orders (OrderID, OrderDate, CustomerID, EmployeeID, StoreID, Status, Updated_at)
    VALUES (1, '2024-01-01', 1, 1, 1, 1, CURRENT_TIMESTAMP),
           (2, '2024-01-05', 1, 1, 1, 3, CURRENT_TIMESTAMP),
           (3, '2024-01-01', 1, 1, 1, 1, CURRENT_TIMESTAMP),
           (4, '2024-02-01', 1, 1, 1, 2, CURRENT_TIMESTAMP),
           (5, '2024-02-01', 1, 1, 1, 1, CURRENT_TIMESTAMP),
           (6, '2024-02-02', 2, 2, 2, 2, CURRENT_TIMESTAMP),
           (7, '2024-01-03', 3, 1, 1, 2, CURRENT_TIMESTAMP),
           (8, '2024-01-03', 3, 1, 2, 2, CURRENT_TIMESTAMP),
           (9, '2024-01-03', 3, 3, 1, 3, CURRENT_TIMESTAMP),
           (10, '2024-01-04', 4, 4, 4, 2, CURRENT_TIMESTAMP),
           (11, '2024-01-05', 5, 5, 5, 3, CURRENT_TIMESTAMP);
    ```  
    Check if dbt is configured properly
    >   dbt debug
    
    If you want to check the customers with highest number of orders by joining orders and customers table and aggregating the result using GroupBy clause run the query 

    ```sql
    SELECT C.CUSTOMERID, CONCAT(C.FIRSTNAME,'', C.LASTNAME) AS CUSTOMERNAME, COUNT (O.ORDERID) AS
    NO_OF_ORDERS
    FROM L1_DEFAULT.CUSTOMERS C
    JOIN L1_DEFAULT.ORDERS O ON C.CUSTOMERID = O.CUSTOMERID
    GROUP BY C.CUSTOMERID, CUSTOMERNAME
    ORDER BY NO_OF_ORDERS DESC;
    ```
    Using CTE's

    ```sql
    WITH CUSTOMERORDERS AS(
        SELECT C.CUSTOMERID, CONCAT(C.FIRSTNAME,'', C.LASTNAME) AS CUSTOMERNAME, COUNT (O.ORDERID) AS NO_OF_ORDERS
        FROM L1_DEFAULT.CUSTOMERS C
        JOIN L1_DEFAULT.ORDERS O ON C.CUSTOMERID = O.CUSTOMERID
        GROUP BY C.CUSTOMERID, CUSTOMERNAME
        ORDER BY NO_OF_ORDERS DESC
    )

    SELECT CUSTOMERID,CUSTOMERNAME,NO_OF_ORDERS
    FROM CUSTOMERORDERS
    ```
- Create a new model file inside models and paste the cte query.
- Run the model
    >dbt run

- To check update in snowflake run the query
    ```sql
    select * from select * from L3_CONSUMPTION.CUSTOMERORDERS
    ```
- You can change the materialization in dbt in specific model     by adding
    >{{config(materialized='table')}}

- For modularity add the model customers_stg and add

    ```sql
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
        L1_DEFAULT.CUSTOMERS    
    ```

    Add  model orders_stg
    ```sql
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
        L1_DEFAULT.ORDERS
    ```
    Add model orderitems_stg
    ```sql
    SELECT
        OrderItemID, 
        orderID, 
        ProductID, 
        Quantity, 
        UnitPrice,
        Quantity * UnitPrice AS TotalPrice,
        Updated_at
    FROM
        L1_DEFAULT.ORDERITEMS
    ```

    Add another intermediate model orders_fact
    ```sql
    SELECT
        O.OrderID,
        O.OrderDate,
        O.CustomerID,
        O.EmployeeID, 
        O.StoreID,
        O.StatusCD,
        O.StatusDesc,
        O.Updated_at,
        COUNT(DISTINCT O.OrderID) AS OrderCount,
        SUM(OI.TotalPrice) AS Revenue
    FROM
        {{ ref( 'orders_stg') }} O
    JOIN
        {{ ref('orderitems_stg') }} OI ON O.OrderID = OI.OrderID
    GROUP BY
        O.OrderID,
        O.OrderDate,
        O.CustomerID,
        O.EmployeeID,
        O.StoreID,
        O.StatusCD,
        O.StatusDesc,
        O.Updated_at
    ```
    Add customerrevenue model
    ```sql
    SELECT
        OS.CustomerID,
        C.CustomerName,
        SUM(OS.OrderCount) AS OrderCount,
        SUM(OS.Revenue) AS REVENUE
    FROM
        {{ ref('orders_fact') }} OS
    JOIN
        {{ref('customers_stg') }} C ON OS.CustomerID = C.CustomerID
    GROUP BY
        OS.CustomerID,
        C.CustomerName
    ```
-   To Override specific model that required table materialization and Override the target schema seemlesly add the macros generate_schema_name

    ```sql
    {% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
    {%- endmacro %}
    ```
    Add the code to your dbt_project.yml file for custom schema for custom tables
    ```
    customers_stg:
      schema: L2_PROCESSING
    orders_stg:
      schema: L2_PROCESSING
    orderitems_stg:
      schema: L2_PROCESSING
    customerrevenue:
      materialized: table
    orders_fact:
      materialized: table
      schema: L2_PROCESSING
    ```
-   To run the customerrevenue model and all preceeding models run
    > dbt run --models +customerrevenue

-   Create a seed file salestarget.csv
    ```
    StoreID, SalesTarget
    1,260000
    2, 290000
    3, 305000
    4,345000
    5, 350000
    ```
-   To seed the file run
    >dbt seed

-   To create a model storeperformance
    ```sql
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
    ```

-   Create a analyses StoreRevenue
    ```sql
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
    ```
-   To compile the ananlyses run the command
    >dbt compile

-   Add source yml file to give identifiers to schemas
    ```yml
    sources:
        - name: default
          database: DBT_WORKSHOP
          schema: L1_DEFAULT
          tables:
            - name: cust
              identifier: customers
            - name: ordr
              identifier: orders
            - name: ordritms
              identifier: orderitems
    ```
- Replace the hard coded source schema and table with source function in customers_stg model

    ```sql
        {{source('default', 'cust')}}
    ```

- Create order_negative_revenue_check test for singularity test

    ```sql
    SELECT OrderID
    FROM {{ ref('orders_fact') }}
    WHERE REVENUE < 0
    ```
- To validate the test run command
    > dbt test

-   To update ORDERS_FACT table to have negative revenue run the sql query on your snowflake account
    ```sql
    UPDATE ORDERS_FACT
    SET REVENUE = REVENUE* (-1)
    WHERE ORDERID IN ('1','2')
    ```
-   Create a generic folder inside tests folder and create a sql file string_not_empty
    ```
    string_not_empty.sql
    {% test string_not_empty(model, column_name) %}
    select {{ column_name }}
    from  {{model}}
    where TRIM({{ column_name}}) = ''
    {% endtest %}
    ```

- Create a new yml file inside models workshop_config.yml
```yml
models:
  - name: customers_stg
    columns:
      - name: Email
        tests:
          - string_not_empty
  - name: employees_stg
    columns:
      - name: JobTitle
        tests:
          - string_not_empty
  - name: orders_stg
    columns:
      - name: OrderID
        tests:
          - unique
          - not_null
      - name: StatusCD
        tests:
          - accepted_values:
              values: ['01', '02', '03']
  - name: orderitems_stg
    columns:
      - name: OrderID
        tests:
          - relationships:
              to: ref( 'orders_stg' )
              field: OrderID       
```
-   To test for freshness add to src_workshop yml file
    ```yml
    freshness:
      warn_after: {count: 1, period: day}
      error_after: {count: 3, period: day}
    loaded_at_field: Updated_at
    ```
    Run the command to test for dbt source freshness
    >dbt source freshness

    To add documentation block add workshop_doc_block.md inside models folder
    ```
    {% docs StatusCD %}

    One of the following values:

    | status    | defination                        |
    |-----------|-----------------------------------|
    | 01        | Order In Progress                 |
    | 02        | Order has been Completed          |
    | 03        | Order has been Cancelled          |

    {% enddocs %}
    ```
    Add the doc block to your yml file and run
    >dbt docs generate

    To run as web run
    >dbt docs serve

-   To write a sample jinja code make a sql file inside models
    ```sql
    {# This Jinja code generates SELECT statements to print numbers from 0 to 9 #}
    {% set max_number = 10 %}
    {% for i in range (max_number) %}
    SELECT {{i}} AS number
    {% if not loop.last %}
    UNION
    {% endif %}
    {% endfor %}
    ```
-   To create minimum record check make a file record_count_check.sql inside tests folder
    ```
    {% set expected_counts = {
        'cust': 2,
        'ordritms': 3,
        'ordr': 2
    } %}

    {% for table, expected_count in expected_counts.items() %}
        SELECT '{{ table }}' AS table_name,
            (SELECT COUNT(*) FROM {{ source( 'default', table) }}) AS record_count,
            {{ expected_count }} AS expected_count
    WHERE (SELECT COUNT(*) FROM {{ source( 'default', table) }}) < {{ expected_count }}
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
    ```
-   To run the jinja test run the command
    >dbt test --select record_count_check

- To understand about macros run the SQL query in your snowflake worksheet
    ```sql
    CREATE TABLE IF NOT EXISTS SALES_NEPAL (
    SALES_DATE DATE NOT NULL,
    PRODUCT_ID INT NOT NULL,
    PRODUCT_CATEGORY_ID INT NOT NULL,
    QUANTITY_SOLD INT NOT NULL,
    UNIT_SELL_PRICE DECIMAL(10,2) NOT NULL,
    UNIT_PURCHASE_COST DECIMAL(10,2) NOT NULL);

    CREATE TABLE IF NOT EXISTS SALES_INDIA (
    SALES_DATE DATE NOT NULL,
    PRODUCT_ID INT NOT NULL,
    PRODUCT_CATEGORY_ID INT NOT NULL,
    QUANTITY_SOLD INT NOT NULL,
    UNIT_SELL_PRICE DECIMAL(10,2) NOT NULL,
    UNIT_PURCHASE_COST DECIMAL(10,2) NOT NULL);
    ```
    Add dummy data inside the tables with following SQL query

    ```sql
    INSERT INTO SALES_NEPAL (SALES_DATE, PRODUCT_ID, PRODUCT_CATEGORY_ID, QUANTITY_SOLD, UNIT_SELL_PRICE, UNIT_PURCHASE_COST)
    VALUES 
    ('2024-01-01', 1, 100, 10, 9.99, 5.00),
    ('2024-01-02', 2, 101, 15, 19.99, 10.00),
    ('2024-01-03', 3, 102, 20, 29.99, 15.00),
    ('2024-01-04', 4, 103, 25, 39.99, 20.00),
    ('2024-01-05', 5, 104, 30, 49.99, 25.00);

    INSERT INTO SALES_INDIA (SALES_DATE, PRODUCT_ID, PRODUCT_CATEGORY_ID, QUANTITY_SOLD, UNIT_SELL_PRICE, UNIT_PURCHASE_COST)
    VALUES 
    ('2024-01-01', 1, 100, 10, 9.99, 3.00),
    ('2024-01-02', 2, 101, 15, 19.99, 6.00),
    ('2024-01-03', 3, 102, 20, 29.99, 9.00),
    ('2024-01-04', 4, 103, 25, 39.99, 12.00),
    ('2024-01-05', 5, 104, 30, 49.99, 15.00);
    ```
-   Add the following code to models in dbt_project.yml file
    ```yml
    sales_nepal:
      schema: L3_CONSUMPTION
      materialized: table
    sales_india:
      schema: L3_CONSUMPTION
      materialized: table
    ```
    Create a new macros workshop_common 
    ```sql
    {% macro generate_profit_model(table_name) %}
    SELECT
    sales_date,
    SUM (quantity_sold * unit_sell_price) as total_revenue,
    SUM(quantity_sold * unit_purchase_cost) as total_cost,
    SUM(quantity_sold * unit_sell_price) - SUM(quantity_sold * unit_purchase_cost) as total_profit
    FROM {{ source('default', table_name) }}
    GROUP BY sales_date
    {% endmacro %}
    ```
-   Create two models profit_nepal & profit_india and add
    > {{ generate_profit_model('sales_nepal') }}

    > {{ generate_profit_model('sales_india') }}

-   Add the following in src_workshop.yml
    ```yml
    - name: sales_nepal
        identifier: sales_nepal
    - name: sales_india
        identifier: sales_india  
    ```

-   Run the command 
    > dbt run --select profit_nepal profit_india

-   Add the code to your packages.yml file to add dbt-utils package
    ```
    packages:
        - package: dbt-labs/dbt_utils
          version: 1.1.1
    ```
- Run the command
  >dbt deps

-   Create a model orderitems_unique
    ```sql
    {{ dbt_utils.deduplicate(
        relation=source('default', 'orderitems'),
        partition_by='orderid',
        order_by="updated_at desc",
       )
    }}
    ```
- Run the command
    >dbt run --select orderitems_unique

- For ephemeral materialization add the code to your model
    ```sql
    {{ config(materialized='ephemeral')}}
    ```
    Run the command
    >dbt run --select customers_stg

- For incremental materialization add the code to your model
    ```sql
    {{ config(materialized='ephemeral')}}
    ```
    ```
    {% if is_incremental() %}
    where Updated_at >= (select max(dbt_updated_at) from {{ this }})
    {% endif %}
    ```

-   For snapshot materialization add customers_history snapshot
    ```sql
    {% snapshot customers_history %}
    {{
        config(
        target_schema='l3_consumption',
        unique_key='CUSTOMERID', 
        strategy='timestamp',
        updated_at= 'updated_at',
        )
    }}

    SELECT * FROM {{ source('default', 'cust') }}
    {%endsnapshot %}
    ```
    Run the command 
    >dbt snapshot