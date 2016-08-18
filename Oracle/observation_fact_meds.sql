/* Building the prescribing table uses a lot of temp tablespace (at KUMC at 
least).  The function that builds the prescribing table failed even when we 
provided 1TB of space.  The error we get is like:

ERROR at line 1:
ORA-12801: error signaled in parallel query server P003
ORA-01652: unable to extend temp segment by 32 in tablespace TEMP
ORA-06512: at "PCORNET_CDM.PCORNETPRESCRIBING", line 53
ORA-06512: at "PCORNET_CDM.PCORNETLOADER", line 12
ORA-06512: at line 2

So, let's make a separate fact table with just medication facts and have the
i2p-transform code reference that table for prescribing.

We (KUMC) have over 2 billion rows in our fact table - only about 234 million are 
medications.  So, hopefully having a table that's an order of magnitude smaller
will help Oracle complete the query using less system resources.
*/
whenever sqlerror continue;
drop index MED_OBS_FACT_ENC_NUM_BI;
drop index MED_OBS_FACT_PAT_NUM_BI;
drop index MED_OBS_FACT_CON_CODE_BI;
drop index MED_OBS_FACT_VALTYP_CD_BI;
drop index MED_OBS_FACT_NVAL_NUM_BI;
drop index MED_OBS_FACT_MOD_CODE_BI;
whenever sqlerror exit;

-- Drop/create a fact table for medications
whenever sqlerror continue;
drop table observation_fact_meds;
whenever sqlerror exit;

create table observation_fact_meds as 
  select * from "&&i2b2_data_schema".observation_fact where 1=0;

-- Insert medication facts
insert into observation_fact_meds
select * from "&&i2b2_data_schema".observation_fact 
where concept_cd like 'KUH|MEDICATION_ID:%';
commit;

-- Build indexes like https://informatics.kumc.edu/work/browser/heron_load/i2b2_facts_index.sql
create bitmap index MED_OBS_FACT_ENC_NUM_BI 
 on observation_fact_meds (ENCOUNTER_NUM)
 nologging parallel 6;

create bitmap index MED_OBS_FACT_PAT_NUM_BI 
 on observation_fact_meds (PATIENT_NUM)
 nologging parallel 6;

create bitmap index MED_OBS_FACT_CON_CODE_BI 
 on observation_fact_meds (CONCEPT_CD)
 nologging parallel 6;

create bitmap index MED_OBS_FACT_VALTYP_CD_BI 
 on observation_fact_meds (VALTYPE_CD)
 nologging parallel 6;

create bitmap index MED_OBS_FACT_NVAL_NUM_BI 
 on observation_fact_meds (NVAL_NUM)
 nologging parallel 6;

create bitmap index MED_OBS_FACT_MOD_CODE_BI 
 on observation_fact_meds (MODIFIER_CD)
 nologging parallel 6;
