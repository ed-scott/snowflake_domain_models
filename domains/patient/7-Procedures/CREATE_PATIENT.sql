USE DATABASE FOUNDATION_{{dbname}};
USE SCHEMA PATIENT;
CREATE OR REPLACE PROCEDURE CREATE_PATIENT("TEMP_TABLE" VARCHAR(250))
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='This procedure contains the specific logic that is used to match patients.'
EXECUTE AS CALLER
AS 
$$

function update_patient_links() {
    snowflake.execute({sqlText:`
        UPDATE PATIENT.PATIENT_LINKS
        SET PATIENT_ID = PATIENT_MATCHED.PATIENT_ID,
            LAST_MODIFIED_TS = PATIENT_MATCHED.LAST_MODIFIED_TS,
            LAST_MODIFIED_BY = 'CREATE_PATIENT Procedure - ${TEMP_TABLE}'
        FROM PATIENT_MATCHED
        WHERE PATIENT_LINKS.ID = LINK_ID`})
}

function update_patient_master()    {
    snowflake.execute({sqlText:`
        UPDATE PATIENT.PATIENT_MASTER
        SET NHS_NUMBER =        COALESCE(PATIENT_MATCHED.NHS_NUMBER, PATIENT_MASTER.NHS_NUMBER),
            FIRST_NAME             = COALESCE(PATIENT_MATCHED.FIRST_NAME, PATIENT_MASTER.FIRST_NAME),
            LAST_NAME              = COALESCE(PATIENT_MATCHED.LAST_NAME, PATIENT_MASTER.LAST_NAME),
            DOB                     = COALESCE(PATIENT_MATCHED.DOB, PATIENT_MASTER.DOB),
            POST_CODE               = COALESCE(PATIENT_MATCHED.POST_CODE, PATIENT_MASTER.POST_CODE),
            EMAIL_ADDRESS           = COALESCE(PATIENT_MATCHED.EMAIL_ADDRESS, PATIENT_MASTER.EMAIL_ADDRESS),
            LAST_MODIFIED_TS        = COALESCE(PATIENT_MATCHED.LAST_MODIFIED_TS, PATIENT_MASTER.LAST_MODIFIED_TS),
            LAST_MODIFIED_BY        = 'CREATE_PATIENT Procedure - ${TEMP_TABLE}'
        FROM PATIENT_MATCHED
        WHERE PATIENT_MATCHED.PATIENT_ID = PATIENT_MASTER.ID
            AND PATIENT_MASTER.LAST_MODIFIED_TS < PATIENT_MATCHED.LAST_MODIFIED_TS`})
}

    //  ******************************* NHS NUMBER MATCHING *******************************
function nhs_number_matching()  { 
    // Check if patient can be matched on NHS Number, if so, update the links table
    snowflake.execute({sqlText:`
        CREATE OR REPLACE TEMPORARY TABLE PATIENT_MATCHED
        AS
        SELECT PATIENT_MASTER.ID PATIENT_ID,${TEMP_TABLE}.*
        FROM DOMAIN_CONFIG.${TEMP_TABLE}
            JOIN PATIENT.PATIENT_MASTER ON PATIENT_MASTER.NHS_NUMBER = ${TEMP_TABLE}.NHS_NUMBER
        `})

    // Update links with the PATIENT_ID for the record
    update_patient_links()

    // Update Records if the last modified date is greater than the current record
    update_patient_master()

    // Then delete the records from the temp table
    snowflake.execute({sqlText:`
        DELETE FROM DOMAIN_CONFIG.${TEMP_TABLE}
        WHERE EXISTS (SELECT 1 FROM PATIENT_MATCHED
                            JOIN PATIENT.PATIENT_MASTER ON PATIENT_MASTER.NHS_NUMBER = PATIENT_MATCHED.NHS_NUMBER
                        WHERE PATIENT_MATCHED.LINK_ID = ${TEMP_TABLE}.LINK_ID)`})
}

    //  ******************************* FIRST_NAME, LAST_NAME, DOB, POST_CODE MATCHING *******************************
function name_dob_postcode_matching() {
    // Check if patient can be matched on First Name, Last Name, DOB and Post Code. 
    // If so, update the links table
    snowflake.execute({sqlText:`
        CREATE OR REPLACE TEMPORARY TABLE PATIENT_MATCHED
        AS
        SELECT PATIENT_MASTER.ID PATIENT_ID,${TEMP_TABLE}.*
        FROM DOMAIN_CONFIG.${TEMP_TABLE}
            JOIN PATIENT.PATIENT_MASTER ON PATIENT_MASTER.FIRST_NAME = ${TEMP_TABLE}.FIRST_NAME
                                        AND PATIENT_MASTER.LAST_NAME = ${TEMP_TABLE}.LAST_NAME
                                        AND PATIENT_MASTER.DOB = ${TEMP_TABLE}.DOB
                                        AND PATIENT_MASTER.POST_CODE = ${TEMP_TABLE}.POST_CODE
        `})

    // Update links with the PATIENT_ID for the record
    update_patient_links()

    // Update Records if the last modified date is greater than the current record
    update_patient_master()

    // Then delete the records from the temp table
    snowflake.execute({sqlText:`
        DELETE FROM DOMAIN_CONFIG.${TEMP_TABLE}
        WHERE EXISTS (SELECT 1 FROM PATIENT_MATCHED
                            JOIN PATIENT.PATIENT_MASTER ON PATIENT_MASTER.FIRST_NAME = PATIENT_MATCHED.FIRST_NAME
                                                        AND PATIENT_MASTER.LAST_NAME = PATIENT_MATCHED.LAST_NAME
                                                        AND PATIENT_MASTER.DOB = PATIENT_MATCHED.DOB
                                                        AND PATIENT_MASTER.POST_CODE = PATIENT_MATCHED.POST_CODE
                        WHERE PATIENT_MATCHED.LINK_ID = ${TEMP_TABLE}.LINK_ID)`})
}
    //  ******************************* EMAIL & DOB MATCHING *******************************
function email_dob_matching() {
    // Check if patient can be matched on Email & DOB, if so, update the links table
    snowflake.execute({sqlText:`
        CREATE OR REPLACE TEMPORARY TABLE PATIENT_MATCHED
        AS
        SELECT PATIENT_MASTER.ID PATIENT_ID,${TEMP_TABLE}.*
        FROM DOMAIN_CONFIG.${TEMP_TABLE}
            JOIN PATIENT.PATIENT_MASTER ON PATIENT_MASTER.EMAIL_ADDRESS = ${TEMP_TABLE}.EMAIL_ADDRESS
                                        AND PATIENT_MASTER.DOB = ${TEMP_TABLE}.DOB
        `})

    // Update links with the PATIENT_ID for the record
    update_patient_links()

    // Update Records if the last modified date is greater than the current record
    update_patient_master()

    // Then delete the records from the temp table
    snowflake.execute({sqlText:`
        DELETE FROM DOMAIN_CONFIG.${TEMP_TABLE}
        WHERE EXISTS (SELECT 1 FROM PATIENT_MATCHED
                            JOIN PATIENT.PATIENT_MASTER ON PATIENT_MASTER.EMAIL_ADDRESS = PATIENT_MATCHED.EMAIL_ADDRESS
                                                        AND PATIENT_MASTER.DOB = PATIENT_MATCHED.DOB
                        WHERE PATIENT_MATCHED.LINK_ID = ${TEMP_TABLE}.LINK_ID)`})
}

    //  ******************************* INITIAL MATCHING *******************************
    nhs_number_matching()
    name_dob_postcode_matching()
    email_dob_matching()

    //  ******************************* NEW RECORDS *******************************
    // Create a new patient matched table for nhs number matching
    snowflake.execute({sqlText:`
        CREATE OR REPLACE TEMPORARY TABLE PATIENT_MATCHED
        AS
        SELECT ${TEMP_TABLE}.*
        FROM DOMAIN_CONFIG.${TEMP_TABLE}
        `})

    // Find max ID from master table to only update new records inserted
    snowflake.execute({sqlText:`SET CURRENT_TS = CURRENT_TIMESTAMP()`})
    
    //  ******************************* NHS NUMBER INSERTS *******************************
    // For any remaining records, create a new record in the master table for NHS_NUMBER 
    snowflake.execute({sqlText:`
        INSERT INTO PATIENT.PATIENT_MASTER (
        NHS_NUMBER, FIRST_NAME, LAST_NAME, DOB, POST_CODE, EMAIL_ADDRESS, LAST_MODIFIED_TS, LAST_MODIFIED_BY)
        SELECT DISTINCT NHS_NUMBER,
                FIRST_VALUE(FIRST_NAME)             OVER (PARTITION BY NHS_NUMBER ORDER BY LAST_MODIFIED_TS DESC),
                FIRST_VALUE(LAST_NAME)              OVER (PARTITION BY NHS_NUMBER ORDER BY LAST_MODIFIED_TS DESC),
                FIRST_VALUE(DOB)                    OVER (PARTITION BY NHS_NUMBER ORDER BY LAST_MODIFIED_TS DESC),
                FIRST_VALUE(POST_CODE)              OVER (PARTITION BY NHS_NUMBER ORDER BY LAST_MODIFIED_TS DESC),
                FIRST_VALUE(EMAIL_ADDRESS)          OVER (PARTITION BY NHS_NUMBER ORDER BY LAST_MODIFIED_TS DESC),
                FIRST_VALUE(LAST_MODIFIED_TS)       OVER (PARTITION BY NHS_NUMBER ORDER BY LAST_MODIFIED_TS DESC),
                'CREATE_PATIENT Procedure - ${TEMP_TABLE}'
        FROM PATIENT_MATCHED
        WHERE NHS_NUMBER IS NOT NULL
        `})

    //Redo matching for additional records
    nhs_number_matching()
    name_dob_postcode_matching()
    email_dob_matching()
    
    //  ******************************* NAME DOB POSTCODE INSERTS *******************************
    // Create a new patient matched table for name, dob and post code matching
    snowflake.execute({sqlText:`
        CREATE OR REPLACE TEMPORARY TABLE PATIENT_MATCHED
        AS
        SELECT ${TEMP_TABLE}.*
        FROM DOMAIN_CONFIG.${TEMP_TABLE}
        `})
        
    // For any remaining records, create a new record in the master table
    snowflake.execute({sqlText:`
        INSERT INTO PATIENT.PATIENT_MASTER (
        NHS_NUMBER, FIRST_NAME, LAST_NAME, DOB, POST_CODE, EMAIL_ADDRESS, LAST_MODIFIED_TS, LAST_MODIFIED_BY)
        SELECT DISTINCT
                NHS_NUMBER,
                FIRST_NAME,
                LAST_NAME,
                DOB,
                POST_CODE,
                FIRST_VALUE(EMAIL_ADDRESS)           OVER (PARTITION BY FIRST_NAME, LAST_NAME, DOB, POST_CODE ORDER BY LAST_MODIFIED_TS DESC),
                FIRST_VALUE(LAST_MODIFIED_TS)        OVER (PARTITION BY FIRST_NAME, LAST_NAME, DOB, POST_CODE ORDER BY LAST_MODIFIED_TS DESC),
                'CREATE_PATIENT Procedure - ${TEMP_TABLE}'
        FROM PATIENT_MATCHED
        WHERE NHS_NUMBER IS NULL
            AND FIRST_NAME IS NOT NULL
            AND LAST_NAME IS NOT NULL
            AND DOB IS NOT NULL
            AND POST_CODE IS NOT NULL
        `})

    //Redo matching for additional records
    name_dob_postcode_matching()
    email_dob_matching()
        
    //  ******************************* EMAIL DOB INSERTS *******************************
    // Create a new patient matched table for email and dob matching
    snowflake.execute({sqlText:`
        CREATE OR REPLACE TEMPORARY TABLE PATIENT_MATCHED
        AS
        SELECT ${TEMP_TABLE}.*
        FROM DOMAIN_CONFIG.${TEMP_TABLE}
        `})

    // For any remaining records, create a new record in the master table
    snowflake.execute({sqlText:`
        INSERT INTO PATIENT.PATIENT_MASTER (
        NHS_NUMBER, FIRST_NAME, LAST_NAME, DOB, POST_CODE, EMAIL_ADDRESS, LAST_MODIFIED_TS, LAST_MODIFIED_BY)
        SELECT DISTINCT NHS_NUMBER,
                FIRST_NAME,
                LAST_NAME,
                DOB,
                POST_CODE,
                EMAIL_ADDRESS,
                FIRST_VALUE(LAST_MODIFIED_TS)        OVER (PARTITION BY FIRST_NAME, EMAIL_ADDRESS, DOB ORDER BY LAST_MODIFIED_TS DESC),
                'CREATE_PATIENT Procedure - ${TEMP_TABLE}'
        FROM PATIENT_MATCHED
        WHERE NHS_NUMBER IS NULL
            AND FIRST_NAME IS NULL
            AND LAST_NAME IS NULL
            AND POST_CODE IS NULL
            AND DOB IS NOT NULL
            AND EMAIL_ADDRESS IS NOT NULL
        `})

    //Redo matching for additional records
    email_dob_matching()

$$
;