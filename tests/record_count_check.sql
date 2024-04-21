-- Define the expected record counts for each table
{% set expected_counts = {
    'cust': 20,
    'ordritms': 3,
    'ordr': 2
} %}

-- Test the count of records in each table 
{% for table, expected_count in expected_counts.items() %}
    SELECT '{{ table }}' AS table_name,
        (SELECT COUNT(*) FROM {{ source( 'default', table) }}) AS record_count,
        {{ expected_count }} AS expected_count
WHERE (SELECT COUNT(*) FROM {{ source( 'default', table) }}) < {{ expected_count }}
{% if not loop.last %} UNION ALL {% endif %}
{% endfor %}