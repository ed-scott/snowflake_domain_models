USE DATABASE FOUNDATION_{{dbname}};
USE SCHEMA DOMAIN_CONFIG;
CREATE OR REPLACE PROCEDURE CREATE_DOMAIN_ITEMS(DOMAIN_ID VARCHAR(100))
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='This procedure facilitates the creation of the specific domain items (e.g. Patients), and calculates age at treatment start and latest discharge date.'
EXECUTE AS CALLER
AS 
$$
    domain_id = DOMAIN_ID

    //Get domain name using domain ID
    domain_result = snowflake.execute({sqlText:`SELECT NAME FROM DOMAIN_CONFIG.DOMAINS WHERE ID = ${domain_id}`})
    domain_result.next()
    domain_name = domain_result.getColumnValue('NAME')

    // Get list of sources to loop through in priority order
    source_result = snowflake.execute({
      sqlText: `
        SELECT * EXCLUDE(ENABLED,PRIORITY)
        FROM ${domain_name}.SOURCES
        WHERE ENABLED = TRUE
        ORDER BY PRIORITY
      `
    })

    // Loop through sources and build a list of dictionaries
    colCount = source_result.getColumnCount()
    source_list = []
    while (source_result.next())  {
      source_dict = {}
      source_field_dict = {}
      for (var i = 1; i <= colCount; i++)  {
        current_col_name = source_result.getColumnName(i)
        current_value = source_result.getColumnValue(i)
        if (current_col_name.includes("FIELD")) {
          source_field_dict[current_col_name] = current_value
        }
        else  {
          source_dict[current_col_name] = current_value
        }
      }
      source_dict["fields"] = source_field_dict
      source_list.push(source_dict)
    }

    // Loop through the list of sources, 
    for (var i = 0; i < source_list.length; i++)  {
      current_source    = source_list[i]
      source_id         = current_source['ID']
      source_name       = current_source['NAME']
      root_table_schema = current_source['ROOT_TABLE_SCHEMA']
      root_view_name   = current_source['ROOT_VIEW_NAME']
      unmatchable_logic = current_source['UNMATCHABLE_LOGIC']
      fields            = current_source['fields']
      uid_field         = current_source['fields']['UID_FIELD']
      field_keys        = Object.keys(fields)
      domain_fields     = ''

      for (var j = 0; j < field_keys.length; j++)  {
        current_key   = field_keys[j]
        current_value = fields[current_key]
        field_alias   = current_key.replace('_FIELD','')

        domain_fields += current_value + ' AS ' + field_alias + ',\r\n'
      }

      // Get max last_modified_ts for source from master TABLE
      max_lmts_result = snowflake.execute({
        sqlText: `
          SELECT TO_VARCHAR(
                   IFNULL(MAX(${domain_name}_MASTER.LAST_MODIFIED_TS), '1900-01-01'),
                   'yyyy-mm-dd HH:MM:SS'
                 )
          FROM ${domain_name}.${domain_name}_MASTER
          JOIN ${domain_name}.${domain_name}_LINKS
            ON ${domain_name}_MASTER.ID = PATIENT_ID
          WHERE SOURCE_ID = ${source_id}
        `
      })
      max_lmts_result.next()
      max_lmts = max_lmts_result.getColumnValue(1)

      // Create temporary table for current source, containing all comparison fields + new fields
      snowflake.execute({
        sqlText: `
          CREATE OR REPLACE TEMPORARY TABLE DOMAIN_CONFIG.${domain_name}_${source_name}_TEMP AS
          SELECT
            LINKS.ID                                      AS LINK_ID,
            ${domain_fields}
          FROM ${domain_name}.${domain_name}_LINKS LINKS
          JOIN {{dbname}}.${root_table_schema}.${root_view_name} ROOT
            ON LINKS.UID = TO_VARCHAR(${uid_field})
          WHERE LINKS.SOURCE_ID = ${source_id}
            AND (LINKS.${domain_name}_ID IS NULL
                 OR LINKS.LAST_MODIFIED_TS > '${max_lmts}')
        `
      })

      // Delete records which do not have sufficient details to match
      snowflake.execute({
        sqlText: `
          DELETE FROM DOMAIN_CONFIG.${domain_name}_${source_name}_TEMP
          WHERE ${unmatchable_logic}
        `
      })

      // Call domain procedure to create / update records
      snowflake.execute({
        sqlText: `
          CALL ${domain_name}.CREATE_${domain_name}('${domain_name}_${source_name}_TEMP')
        `
      })
    }
$$
;