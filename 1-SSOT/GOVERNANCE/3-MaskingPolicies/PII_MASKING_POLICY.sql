USE DATABASE SSOT;
USE SCHEMA GOVERNANCE;

--Remove the masking policy from the tag
EXECUTE IMMEDIATE $$
BEGIN 
LET count NUMBER(38,0) := (select COUNT(*) 
                            from table(information_schema.policy_references(ref_entity_domain => 'TAG',
                                                                            ref_entity_name=> 'GOVERNANCE.PROTECTED_PII'))
                            WHERE POLICY_NAME = 'PII_MASKING_POLICY');
IF (count > 0) THEN
ALTER TAG PROTECTED_PII UNSET MASKING POLICY PII_MASKING_POLICY;
END IF;
END;
$$
;

--Update tag-based masking policy
CREATE OR REPLACE MASKING POLICY PII_MASKING_POLICY
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN (CURRENT_ROLE()) IN ('ACCOUNTADMIN','OTHER_UNMASKED_ROLE')
        THEN val
    ELSE
        CASE
            WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('PROTECTED_PII') = 'DEFAULT'
                THEN '***MASKED***'
            --Allows first character to be shown
            WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('PROTECTED_PII') IN ('DEFAULT_INITIAL')
                THEN REGEXP_REPLACE(INITCAP(val), $$[a-z0-9]$$, $$*$$, 1, 0, $$c$$)
            ELSE
                val
        END
    END
COMMENT='Tag value determines masking type and allows admin roles to bypass masking';

--apply the policy back to the Tag
ALTER TAG PROTECTED_PII SET MASKING POLICY PII_MASKING_POLICY;