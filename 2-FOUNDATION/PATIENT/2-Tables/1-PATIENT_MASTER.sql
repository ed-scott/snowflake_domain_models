USE DATABASE FOUNDATION;
USE SCHEMA PATIENT;

CREATE TABLE IF NOT EXISTS PATIENT_MASTER (
  ID                          NUMBER(38,0)   NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
  NHS_NUMBER                  NUMBER(38,0)   WITH TAG (SSOT.GOVERNANCE.PROTECTED_PII='DEFAULT_KEEPLENGTH'),
  FIRST_NAME                  VARCHAR(100)   WITH TAG (SSOT.GOVERNANCE.PROTECTED_PII='DEFAULT_FIRSTNAME'),
  LAST_NAME                   VARCHAR(100)   WITH TAG (SSOT.GOVERNANCE.PROTECTED_PII='DEFAULT_LASTNAME'),
  DOB                         DATE           WITH TAG (SSOT.GOVERNANCE.PROTECTED_PII='DEFAULT_DATE'),
  POST_CODE                   VARCHAR(100)   WITH TAG (SSOT.GOVERNANCE.PROTECTED_PII='DEFAULT'),
  EMAIL_ADDRESS               VARCHAR(320)   WITH TAG (SSOT.GOVERNANCE.PROTECTED_PII='DEFAULT_EMAIL'),
  TREATMENT_START_DATE        DATE,                   -- Calculated using Metadata
  LATEST_DISCHARGE_DATE       DATE,                   -- Calculated using Metadata
  CHILD_RETENTION_DATE        DATE,                   -- Do we need both Child and Adult???
  ADULT_RETENTION_DATE        DATE,
  AGE_AT_TREATMENT_START      NUMBER(38,0),           --To remove - can be calculated using DOB and TREATMENT_START_DATE
  IS_DECEASED                 BOOLEAN DEFAULT FALSE,  --Potentially remove as covered by DOD
  DATE_OF_DEATH               VARCHAR(16777216),      -- Calculated using Metadata
  IS_MENTAL_HEALTH_RECORD     BOOLEAN DEFAULT TRUE,   -- Calculated using Metadata
  LEGAL_HOLD                  BOOLEAN DEFAULT FALSE,
  CATEGORY                    VARCHAR(16777216),
  LAST_MODIFIED_TS            TIMESTAMP_NTZ(9),
  LAST_MODIFIED_BY            VARCHAR(100),
  METADATA_LAST_MODIFIED_TS   TIMESTAMP_NTZ(9)
);

INSERT INTO PATIENT_MASTER (ID,
                            NHS_NUMBER,
                            FIRST_NAME,
                            LAST_NAME,
                            DOB,
                            POST_CODE,
                            EMAIL_ADDRESS,
                            TREATMENT_START_DATE,
                            LATEST_DISCHARGE_DATE,
                            CHILD_RETENTION_DATE,
                            ADULT_RETENTION_DATE,
                            LAST_MODIFIED_TS,
                            LAST_MODIFIED_BY)
SELECT  ID,
        NHS_NUMBER,
        FIRST_NAME,
        LAST_NAME,
        DOB,
        POST_CODE,
        EMAIL_ADDRESS,
        TREATMENT_START_DATE,
        LATEST_DISCHARGE_DATE,
        CHILD_RETENTION_DATE,
        ADULT_RETENTION_DATE,
        LAST_MODIFIED_TS,
        LAST_MODIFIED_BY
FROM PATIENT_MASTER_PREVIOUS;

DROP TABLE IF EXISTS PATIENT_MASTER_PREVIOUS;

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.ID
  IS 'Auto‑incrementing unique identifier for each patient record.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.NHS_NUMBER
  IS 'National Health Service (NHS) identifier for the patient.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.FIRST_NAME
  IS 'Patients first name.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.LAST_NAME
  IS 'Patients last name.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.DOB
  IS 'Date of birth of the patient.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.POST_CODE
  IS 'Patients postal code.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.EMAIL_ADDRESS
  IS 'Patients email address.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.TREATMENT_START_DATE
  IS 'Date when the patients treatment began.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.LATEST_DISCHARGE_DATE
  IS 'Most recent discharge date for the patient.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.CHILD_RETENTION_DATE
  IS 'Date until which data must be retained under child privacy rules.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.ADULT_RETENTION_DATE
  IS 'Date until which data must be retained under adult privacy rules.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.AGE_AT_TREATMENT_START
  IS 'Patients age in years at the start of treatment.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.IS_DECEASED
  IS 'Flag indicating whether the patient is deceased.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.DATE_OF_DEATH
  IS 'Date of death (if known).';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.IS_MENTAL_HEALTH_RECORD
  IS 'Flag indicating if this is a mental health record.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.LEGAL_HOLD
  IS 'Flag indicating if the record is on legal hold (prevents deletion or masking).';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.CATEGORY
  IS 'Retention category assigned to the patient record.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.LAST_MODIFIED_TS
  IS 'Timestamp of the last modification to the patient record within the source system specified in LAST_MODIFIED_BY.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.LAST_MODIFIED_BY
  IS 'Source system that was used to last update the patient record.';

COMMENT ON COLUMN FOUNDATION.PATIENT.PATIENT_MASTER.METADATA_LAST_MODIFIED_TS
  IS 'Timestamp when the metadata was last modified.';
