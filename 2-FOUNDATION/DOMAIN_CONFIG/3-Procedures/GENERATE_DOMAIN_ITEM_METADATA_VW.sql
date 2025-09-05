USE DATABASE FOUNDATION;
USE SCHEMA DOMAIN_CONFIG;
CREATE OR REPLACE PROCEDURE GENERATE_DOMAIN_ITEM_METADATA_VW (DOMAIN_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER 
AS
$$
    var domain_id = DOMAIN_ID

    //Get domain name using domain ID
    domain_result = snowflake.execute({sqlText:`SELECT NAME FROM DOMAIN_CONFIG.DOMAINS WHERE ID = ${domain_id}`})
    domain_result.next()
    domain_name = domain_result.getColumnValue('NAME')

    // Find the Metadata Dynamic Tables that exist within the specified Domain
    source_result = snowflake.execute({sqlText:`
        SELECT SOURCES.NAME SOURCE_NAME,LISTAGG(CONCAT(AGGREGATION_METHOD,'(',FIELD_NAME,') AS ',FIELD_NAME),',') AGGS
        FROM FOUNDATION.DOMAIN_CONFIG.DOMAIN_ITEM_METADATA_FIELD DIM_FIELD
            JOIN FOUNDATION.DOMAIN_CONFIG.DOMAIN_ITEM_METADATA_CONFIG CONFIG ON CONFIG.METADATA_FIELD_ID = DIM_FIELD.ID
            JOIN FOUNDATION.${domain_name}.SOURCES ON CONFIG.SOURCE_ID = SOURCES.ID
        WHERE DIM_FIELD.DOMAIN_ID = ${domain_id}
        GROUP BY ALL
            `})
    
    // Loop through the tables, adding them to the union statement
    first_table = 1
    union_query = ''
    while (source_result.next())    {
        source_name = source_result.getColumnValue('SOURCE_NAME')
        aggs = source_result.getColumnValue('AGGS')
        if (first_table == 0)   {
            union_query += `
            UNION BY NAME`
        }
        first_table = 0
        union_query += `
            SELECT PATIENT_ID,${aggs}
            FROM ${domain_name}.DT_${source_name}_METADATA
            GROUP BY ALL`
    }

    //Execute DT Creation
    snowflake.execute({sqlText:`
            CREATE OR REPLACE VIEW ${domain_name}.VW_METADATA
            AS ` + union_query})

    //return SELECT statement
    return union_query
$$;
