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

/*
 * NOTE: The below code is a fix for Cycle 2 which replaces deid instance_nums
 * with deid order_ids in i2b2medfacts inorder to allow the SCILHS PRESCRIBING
 * code to merge facts correctly (see PCORNetPrescribing in 
 * PCORNetLoader_ora.sql).
 */

 -- Please excuse the copy and paste from i2b2_facts_deid.sql, but this is
 -- needed for the mapping below (portions of which are also copied from
 -- i2b2_facts_deid.sql).
 create or replace view byte_conv as
 select num_bytes, rpad('x', num_bytes * 2, 'x') as xfill
   from (select 7 as num_bytes from dual);
 
-- Generate deid instance_num order_id mapping
whenever sqlerror continue;
drop table med_order_instance_map;
whenever sqlerror exit;

create table med_order_instance_map as 
with id_med_instance as (
  select instance_num
  from "&&i2b2_id_data_schema".observation_fact@id
  where concept_cd like 'KUH|MEDICATION_ID:%'
),
-- based on instance_num deid in i2b2_facts_deid.sql.
order_id_mapping as (
  select
  to_number(substr(instance_num, -1)) val,
    case when to_number(substr(instance_num, -1)) < 3 then floor(instance_num / 1e5)
    else floor(instance_num / 1e6) end order_id, -- Corrects for bug in HERON ETL
    instance_num,
    to_number(
      rawtohex(
       UTL_RAW.SUBSTR(
        DBMS_CRYPTO.Hash(UTL_RAW.CAST_FROM_NUMBER(instance_num),
                         /* typ 3 is HASH_SH1 */
                         3),
        1, (select num_bytes from byte_conv))),
      (select xfill from byte_conv)) deid_instance_num
  from id_med_instance
),
distinct_order_ids as (
  select distinct order_id from order_id_mapping
),
deid_id_order_id_mapping as (
  select rownum deid_order_id, order_id
  from distinct_order_ids
)
select distinct dim.deid_order_id, oim.deid_instance_num 
from order_id_mapping oim
join deid_id_order_id_mapping dim
  on oim.order_id=dim.order_id
;

-- Updated instance_nums in ib2bmedfact with order_ids
create table observation_fact_meds_2 as
select 
	ofm.ENCOUNTER_NUM,
	ofm.PATIENT_NUM,
	ofm.CONCEPT_CD,
	ofm.PROVIDER_ID,
	ofm.START_DATE,
	ofm.MODIFIER_CD,
	moim.deid_order_id,
	ofm.VALTYPE_CD,
	ofm.TVAL_CHAR,
	ofm.NVAL_NUM,
	ofm.VALUEFLAG_CD,
	ofm.QUANTITY_NUM,
	ofm.UNITS_CD,
	ofm.END_DATE,
	ofm.LOCATION_CD,
	ofm.OBSERVATION_BLOB,
	ofm.CONFIDENCE_NUM,
	ofm.UPDATE_DATE,
	ofm.DOWNLOAD_DATE,
	ofm.IMPORT_DATE,
	ofm.SOURCESYSTEM_CD,
	ofm.UPLOAD_ID,
	ofm.SUB_ENCOUNTER
from observation_fact_meds ofm
join med_order_instance_map moim
	on ofm.instance_num=moim.deid_instance_num
;

whenever sqlerror continue;
drop table observation_fact_meds;
whenever sqlerror exit;

rename table observation_fact_meds_2 to observation_fact_meds;

whenever sqlerror continue;
drop table observation_fact_meds_2;
whenever sqlerror exit;