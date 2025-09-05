USE DATABASE FOUNDATION;
USE SCHEMA DOMAIN_CONFIG;
CREATE OR REPLACE PROCEDURE UPDATE_DOMAIN_ITEM_METADATA (DOMAIN_ID VARCHAR)
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

    //Get fields from metadata tables
    field_result = snowflake.execute({sqlText:`
        SELECT LISTAGG(CONCAT('${domain_name}_MASTER.',FIELD_NAME,' = ','VW_METADATA.',FIELD_NAME),',\r\n') FIELDS
        FROM DOMAIN_CONFIG.DOMAIN_ITEM_METADATA_FIELD
        WHERE DOMAIN_ID = ${domain_id};`})
    field_result.next()
    fields = field_result.getColumnValue('FIELDS')

    //Build update statement using fields
    snowflake.execute({sqlText:`
        UPDATE ${domain_name}.${domain_name}_MASTER
        SET ${fields}
        FROM ${domain_name}.VW_METADATA
        WHERE ${domain_name}_MASTER.ID = VW_METADATA.PATIENT_ID
        ;`})
$$
;