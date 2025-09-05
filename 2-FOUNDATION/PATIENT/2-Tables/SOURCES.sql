USE DATABASE FOUNDATION;
USE SCHEMA PATIENT;
CREATE TABLE IF NOT EXISTS SOURCES (
    ID NUMBER(38,0) AUTOINCREMENT PRIMARY KEY UNIQUE,
    NAME VARCHAR(100),
    ENABLED BOOLEAN,
    PRIORITY NUMBER(10,0),
    ROOT_TABLE_SCHEMA VARCHAR(100),
    ROOT_TABLE_NAME VARCHAR(100),
    ROOT_VIEW_NAME VARCHAR(100),
    ROOT_TABLE_FILTER VARCHAR(1000),
    UNMATCHABLE_LOGIC VARCHAR,  -- This is a WHERE clause that finds any records that don't meet the minimum criteria
                                -- E.g. Patients require at least 
                                -- NHS Number OR First Name, Last Name, DOB, Post Code OR Email and DOB
                                --Exmaple to find records that :
                                -- NHS_NUMBER IS NULL AND 
                                -- (FIRST_NAME IS NULL OR LAST_NAME IS NULL OR DOB IS NULL OR POST_CODE IS NULL)
                                -- AND (EMAIL_ADDRESS IS NULL OR DOB IS NULL)
    UID_FIELD VARCHAR(100),
    LAST_MODIFIED_TS_FIELD VARCHAR(100),
    NHS_NUMBER_FIELD VARCHAR(100),
    FIRST_NAME_FIELD VARCHAR(100),
    LAST_NAME_FIELD VARCHAR(100),
    DOB_FIELD VARCHAR(100),
    POST_CODE_FIELD VARCHAR(100),
    EMAIL_ADDRESS_FIELD VARCHAR(100)
);

/*################## EXAMPLE INSERTS ################## 

INSERT INTO SOURCES (   NAME,
                        ENABLED,
                        PRIORITY,
                        ROOT_TABLE_SCHEMA,
                        ROOT_TABLE_NAME,
                        ROOT_VIEW_NAME,
                        ROOT_TABLE_FILTER,
                        UNMATCHABLE_LOGIC,
                        UID_FIELD,
                        LAST_MODIFIED_TS_FIELD,
                        NHS_NUMBER_FIELD,
                        FIRST_NAME_FIELD,
                        LAST_NAME_FIELD,
                        DOB_FIELD,
                        POST_CODE_FIELD,
                        EMAIL_ADDRESS_FIELD) 
VALUES('NHS_Patient',TRUE,1,'NHS','PATIENT','AND DATE_OF_DEATH IS NULL','NHS_NUMBER IS NULL
AND
(FIRST_NAME IS NULL
OR LAST_NAME IS NULL
OR DOB IS NULL
OR POST_CODE IS NULL)
AND 
(EMAIL_ADDRESS IS NULL
OR DOB IS NULL)','ID','LAST_MODIFIED','NHS_NUMBER','FIRST_NAME','LAST_NAME','DATE_OF_BIRTH','POST_CODE','EMAIL_ADDRESS'),
      ('Nuffield_Health_Patient',TRUE,2,'NUFFIELD','CLIENT',NULL,'NNN IS NULL
AND
(FORENAME IS NULL
OR SURNAME IS NULL
OR DOB IS NULL
OR POST_CODE IS NULL)
AND 
(EMAIL IS NULL
OR DOB IS NULL)','CLIENT_ID','LMD','NNN','FORENAME','SURNAME','DOB','POST_CODE','EMAIL')
;

*/