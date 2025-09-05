USE DATABASE FOUNDATION_{{dbname}};
USE SCHEMA DOMAIN_CONFIG;
CREATE OR REPLACE PROCEDURE CREATE_LINKS (DOMAIN_ID VARCHAR(100))
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = 'This procedure creates all of the links within the specific LINKS table.'
EXECUTE AS CALLER
AS
$$
    domain_id = DOMAIN_ID

    //Get domain name using domain ID
    domain_result = snowflake.execute({sqlText:`SELECT NAME FROM DOMAIN_CONFIG.DOMAINS WHERE ID = ${domain_id}`})
    domain_result.next()
    domain_name = domain_result.getColumnValue('NAME')

    // Get list of sources to loop through in priority order
    source_result = snowflake.execute({sqlText: `
        SELECT ID,NAME,ROOT_TABLE_SCHEMA,ROOT_VIEW_NAME,IFNULL(ROOT_TABLE_FILTER,'') ROOT_TABLE_FILTER,UID_FIELD,LAST_MODIFIED_TS_FIELD
        FROM ${domain_name}.SOURCES`})

    // Loop through sources
    while (source_result.next())    {
        source_id = source_result.getColumnValue(1)
        source_name = source_result.getColumnValue(2)
        root_table_schema = source_result.getColumnValue(3)
        root_view_name = source_result.getColumnValue(4)
        root_table_filter = source_result.getColumnValue(5) == null ? '' : source_result.getColumnValue(5)
        uid_field = source_result.getColumnValue(6)
        last_modified_field = source_result.getColumnValue(7)

        // Add all UIDs to the link table that do not already exist
        snowflake.execute({sqlText:`
            INSERT INTO ${domain_name}.${domain_name}_LINKS (SOURCE_ID,UID,LAST_MODIFIED_TS,LAST_MODIFIED_BY)
            SELECT ${source_id},${uid_field},MAX(${last_modified_field}),'CREATE_LINKS Procedure'
            FROM {{dbname}}.${root_table_schema}.${root_view_name} ROOT
            WHERE NOT EXISTS (SELECT 1 FROM ${domain_name}.${domain_name}_LINKS EXISTING
                                WHERE EXISTING.SOURCE_ID = ${source_id}
                                    AND EXISTING.UID = TO_VARCHAR(${uid_field}))
                ${root_table_filter}
            GROUP BY ALL
            `})

        // Update any existing records with their latest last modified date
        snowflake.execute({sqlText:`
            UPDATE ${domain_name}.${domain_name}_LINKS
            SET ${domain_name}_LINKS.LAST_MODIFIED_TS = ROOT.LAST_MODIFIED_TS,
                ${domain_name}_LINKS.LAST_MODIFIED_BY = 'CREATE_LINKS Procedure'
            FROM (SELECT ${uid_field},MAX(${last_modified_field}) LAST_MODIFIED_TS 
                    FROM {{dbname}}.${root_table_schema}.${root_view_name} ROOT
                    GROUP BY ALL) ROOT
            WHERE SOURCE_ID = ${source_id}
                AND ${domain_name}_LINKS.UID = TO_VARCHAR(${uid_field})
                AND ${domain_name}_LINKS.LAST_MODIFIED_TS < ROOT.LAST_MODIFIED_TS`})
    }
$$
;