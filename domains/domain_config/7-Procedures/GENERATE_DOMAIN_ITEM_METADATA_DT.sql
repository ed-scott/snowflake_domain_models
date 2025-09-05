USE DATABASE FOUNDATION_{{dbname}};
USE SCHEMA DOMAIN_CONFIG;
CREATE OR REPLACE PROCEDURE GENERATE_DOMAIN_ITEM_METADATA_DT (DOMAIN_ID VARCHAR,SOURCE_ID VARCHAR,COUNT_ONLY BOOLEAN,CREATE_DT BOOLEAN)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER 
AS
$$
    var domain_id = DOMAIN_ID
    var source_id = SOURCE_ID

    //Get domain name using domain ID
    domain_result = snowflake.execute({sqlText:`SELECT NAME FROM DOMAIN_CONFIG.DOMAINS WHERE ID = ${domain_id}`})
    domain_result.next()
    domain_name = domain_result.getColumnValue('NAME')
    
    //Get the tables used for the source
    table_links_ordered_statement = snowflake.createStatement({sqlText:`
        SELECT  PK_VIEW_NAME,
                PK_COLUMN_NAME,
                FK_VIEW_NAME,
                FK_COLUMN_NAME,
                ROOT_TABLE_SCHEMA,
                ROOT_VIEW_NAME,
                UID_FIELD,
                SOURCES.NAME SOURCE_NAME,
                SORT_ORDER
        FROM DOMAIN_CONFIG.TABLE_LINKS
            JOIN ${domain_name}.SOURCES ON SOURCES.ID = SOURCE_ID
        WHERE TABLE_LINKS.DOMAIN_ID = ${domain_id}
            AND TABLE_LINKS.SOURCE_ID = ${source_id}
        GROUP BY ALL
        ORDER BY SORT_ORDER;`})

    source_tables_result = table_links_ordered_statement.execute()
    source_tables_result.next()
    root_table_schema = source_tables_result.getColumnValue('ROOT_TABLE_SCHEMA')
    root_view_name = source_tables_result.getColumnValue('ROOT_VIEW_NAME')
    uid_field = source_tables_result.getColumnValue('UID_FIELD')
    source_name = source_tables_result.getColumnValue('SOURCE_NAME')

    // Get the columns for the table if required
    if (COUNT_ONLY == 1)    {
        // Get the columns used to check the counts for each table
        column_result = snowflake.execute({sqlText:`
            SELECT LISTAGG(CONCAT('COUNT(',FK_VIEW_NAME,'.',FK_COLUMN_NAME,') AS ',FK_VIEW_NAME,'$',FK_COLUMN_NAME),',\\r\\n') WITHIN GROUP (ORDER BY SORT_ORDER)
            FROM DOMAIN_CONFIG.TABLE_LINKS
            WHERE SOURCE_ID = ${source_id}
                AND DOMAIN_ID = '${domain_id}';`})
        column_result.next()
        columns = column_result.getColumnValue(1)
    }
    else if (COUNT_ONLY == 0)    {
        column_result = snowflake.execute({sqlText:`
            SELECT LISTAGG(CONCAT(CONF.CALCULATION,' AS ',FIELD.FIELD_NAME),',') CALCULATIONS
            FROM DOMAIN_CONFIG.DOMAIN_ITEM_METADATA_CONFIG CONF
                JOIN DOMAIN_CONFIG.DOMAIN_ITEM_METADATA_FIELD FIELD ON CONF.METADATA_FIELD_ID = FIELD.ID
            WHERE SOURCE_ID = ${source_id}
                AND DOMAIN_ID = ${domain_id};`})
        column_result.next()
        columns = column_result.getColumnValue(1)
    }
    else    {
        return "Error - COUNT_ONLY parameter was not provided correctly. It must be either 1 or 0 (True or False)"
    }

    // Set the start of the Select statement using the columns
    main_query = `
        FROM PATIENT.PATIENT_MASTER PM
            JOIN PATIENT.PATIENT_LINKS PL
                ON TO_VARCHAR(PM.ID) = TO_VARCHAR(PL.PATIENT_ID)
            JOIN {{dbname}}.${root_table_schema}.${root_view_name} ROOT
                ON TO_VARCHAR(${uid_field}) = TO_VARCHAR(PL.UID)`
    
    // Get the table links ordered to allow processing of the JOINs
    table_links_ordered_result = table_links_ordered_statement.execute()

    // While the PK table does not equal to the root, prepend further joins the same way
    while (table_links_ordered_result.next())    {
        pk_view_name = table_links_ordered_result.getColumnValue(1)
        pk_column_name = table_links_ordered_result.getColumnValue(2)
        fk_view_name = table_links_ordered_result.getColumnValue(3)
        fk_column_name = table_links_ordered_result.getColumnValue(4)

        if (root_view_name != fk_view_name) {
            pk_view_name = pk_view_name == root_view_name ? 'ROOT' : pk_view_name
            main_query += `
                LEFT JOIN {{dbname}}.${root_table_schema}.${fk_view_name}
                ON TO_VARCHAR(${pk_view_name}.${pk_column_name})
                    = TO_VARCHAR(${fk_view_name}.${fk_column_name})`
        }
        
    }
    if (CREATE_DT == 1 && COUNT_ONLY == 0)  {
        snowflake.execute({sqlText:`
            CREATE OR REPLACE DYNAMIC TABLE ${domain_name}.DT_${source_name}_METADATA
            TARGET_LAG = '2 hours'
            WAREHOUSE = WH_CLEKT_{{environment}}
            AS SELECT   PM.ID PATIENT_ID,
                        PL.SOURCE_ID,
                        PL.UID,
                        ` + columns + ` ` + main_query + `
            GROUP BY ALL`})
    }
    else if (CREATE_DT == 1 && COUNT_ONLY == 1)  {
        snowflake.execute({sqlText:`
            CREATE OR REPLACE DYNAMIC TABLE ${domain_name}.DT_${source_name}_COUNTS
            TARGET_LAG = '2 hours'
            WAREHOUSE = WH_CLEKT_{{environment}}
            AS SELECT   PM.ID PATIENT_MASTER_ID,
                        PL.SOURCE_ID,
                        PL.UID,
                        ` + columns + ` ` + main_query + `
            GROUP BY ALL`})
    }
    else    {
        return `SELECT ` + columns + ` ` + main_query + `
        GROUP BY ALL`
    }
$$
;