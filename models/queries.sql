WITH columns AS (
    SELECT
        column_name AS cn,
        table_catalog || '.' || table_schema || '.' || table_name AS tn

    /*not working out of box so hardcoding bigquery requirments for looking at info-schema
    for timesake, not trying to get variables working - uneccesary complexity at moment.*/    
    FROM `ele-prod-735126`.`dbt_jasondexter_preparation_layer`.`INFORMATION_SCHEMA.COLUMNS`
    --FROM "{{ var('source-database', target.database) }}".{{ source('information_schema', 'columns').include(database=false) }}
    
    WHERE
    
        -- hardcoding for now
        LOWER(column_name) IN ('athena_id', 'athena_new_id', 'enterprise_id', 'elation_id', 'legacy_patient_id',
                               'lead_id', 'account_id', 'contact_id', 'email')
        {# LOWER(column_name) IN {{ var('id-columns') }} #}

        -- hardcoding for now
        AND LOWER(table_name) IN ('base_athena_members', 'base_elation_members', 'base_opshub_members',
                              'int_athena__pivoted_custom_demographics',
                              'base_salesforce_leads', 'base_salesforce_accounts', 'base_salesforce_contacts')
        {# {% if var('tables-to-include', undefined) %}
        AND LOWER(table_name) IN {{ var('tables-to-include') }}
        {% endif %} #}
        
        -- not using schemas (include) for not
        {# {% if var('schemas-to-include', undefined) %}
        AND LOWER(table_schema) IN {{ var('schemas-to-include') }}
        {% endif %} #}

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
