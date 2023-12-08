WITH columns AS (
    SELECT
        column_name AS cn,
        table_catalog || '.' || table_schema || '.' || table_name AS tn
    FROM `{{ var('source-database', target.database) }}`.`region-us`.{{ source('INFORMATION_SCHEMA', 'COLUMNS').include(database=false) }}
    WHERE
        LOWER(column_name) IN {{ var('id-columns') }}
        {% if var('schemas-to-include', undefined) %}
        AND LOWER(table_schema) IN {{ var('schemas-to-include') }}
        {% endif %}
        {% if var('schemas-to-exclude', undefined) %}
        AND NOT LOWER(table_schema) IN {{ var('schemas-to-exclude') }}
        {% endif %}
        {% if var('tables-to-include', undefined) %}
        AND LOWER(table_name) IN {{ var('tables-to-include') }}
        {% endif %}
        {% if var('tables-to-exclude', undefined) %}
        AND NOT LOWER(table_name) IN {{ var('tables-to-exclude') }}
        {% endif %}
        AND NOT LOWER(table_name) LIKE 'snapshot_%'
        AND NOT LOWER(table_name) LIKE 'sync_data_%'
        AND NOT LOWER(table_name) LIKE 'failed_records_%'
)

SELECT "SELECT DISTINCT CAST("|| a.cn ||" AS STRING) AS node_a, '" || a.cn || "' AS node_a_label, CAST("|| b.cn ||" AS STRING) AS node_b, '" || b.cn || "' AS node_b_label FROM " || a.tn || " WHERE COALESCE(CAST(" || a.cn || " AS STRING), '') != '' AND COALESCE(CAST(" || b.cn || " AS STRING), '') != ''" AS sql_to_run
FROM columns AS a
INNER JOIN columns AS b
    ON a.tn = b.tn
        AND a.cn > b.cn