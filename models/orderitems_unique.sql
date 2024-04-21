{{ dbt_utils.deduplicate(
    relation=source('default', 'ordritms'),
    partition_by='orderid',
    order_by="updated_at desc",
   )
}}