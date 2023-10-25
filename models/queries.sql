WITH columns AS (
    SELECT
        column_name AS cn,
        table_catalog || '.' || table_schema || '.' || table_name AS tn

    /*not working out of box so hardcoding bigquery requirments for looking at info-schema.
    'region-us' looks at ALL of our BQ data-warehouse; we use 'schema' var based on needs.*/    
    FROM `ele-prod-735126`.`region-us`.`INFORMATION_SCHEMA.COLUMNS`
    --FROM "{{ var('source-database', target.database) }}".{{ source('information_schema', 'columns').include(database=false) }}
    
    WHERE
    
        -- get id-columns of interest
        LOWER(column_name) IN {{ var('id-columns') }}

        -- get tables of interest
        {% if var('tables-to-include', undefined) %}
        AND LOWER(table_name) IN {{ var('tables-to-include') }}
        {% endif %}
        
        -- get schema(s) (BQ dataset) to include
        {% if var('schemas-to-include', undefined) %}
        AND LOWER(table_schema) IN {{ var('schemas-to-include') }}
        {% endif %}

        -- not using schemas (exclude) for now
        {# {% if var('schemas-to-exclude', undefined) %}
        AND NOT LOWER(table_schema) IN {{ var('schemas-to-exclude') }}
        {% endif %} #}
        
        -- not using tables (exclude) for now
        {# {% if var('tables-to-exclude', undefined) %}
        AND NOT LOWER(table_name) IN {{ var('tables-to-exclude') }}
        {% endif %} #}

        -- leaving as is
        AND NOT LOWER(table_name) LIKE 'snapshot_%'
        AND NOT LOWER(table_name) LIKE 'sync_data_%'
        AND NOT LOWER(table_name) LIKE 'failed_records_%'

)

-- bigquery specific edits
SELECT "SELECT DISTINCT CAST("|| a.cn ||" AS STRING) AS node_a, '" || a.cn || "' AS node_a_label, CAST("|| b.cn ||" AS STRING) AS node_b, '" || b.cn || "' AS node_b_label FROM " || a.tn || " WHERE COALESCE(CAST(" || a.cn || " AS STRING), '') != '' AND COALESCE(CAST(" || b.cn || " AS STRING), '') != ''" AS sql_to_run
-- SELECT 'SELECT DISTINCT ' || a.cn || '::TEXT AS node_a, ''' || a.cn || ''' AS node_a_label, ' || b.cn || '::TEXT AS node_b, ''' || b.cn || ''' AS node_b_label FROM ' || a.tn || ' WHERE COALESCE(' || a.cn || '::TEXT, '''') != '''' AND COALESCE(' || b.cn || '::TEXT, '''') != ''''' AS sql_to_run

FROM columns AS a
INNER JOIN columns AS b
    ON a.tn = b.tn
        AND a.cn > b.cn
