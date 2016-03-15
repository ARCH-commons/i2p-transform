/** heron_encounter_style -- apply HERON / KUMC / GPC encounter conventions

copyright (c) 2016 University of Kansas Medical Center
availble under the i2b2 Software License (aka MPL?)

*/

/** Index patid, encounterid in hopes of speeding up exploration.

alter session set current_schema = pcornet_cdm;

create unique index demographic_patid on demographic (patid);
create unique index encounter_id on encounter (encounterid);

create index encounter_patid on encounter (patid);
create index diagnosis_patid on diagnosis (patid);
create index diagnosis_encid on diagnosis (encounterid);
create index procedures_patid on procedures (patid);
create index procedures_encid on procedures (encounterid);
create index vital_patid on vital (patid);

actually, foreign key constraints make more sense...

alter table encounter
add constraint fk_patid foreign key (patid)
references demographic (patid);


alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI';

 */


/** get length_of_stay, visit end_date from fact table

TODO: push this back into HERON ETL
*/
merge into "&&i2b2_data_schema".visit_dimension vd
using (
  -- can we get it from UHC length_of_stay?
  select obs.encounter_num, obs.patient_num
       , obs.nval_num length_of_stay
       , obs.end_date discharge_date
  from "&&i2b2_data_schema".observation_fact obs
  where obs.concept_cd = 'UHC|LOS:1'  -- TODO: parameterize
) obs
on (  obs.encounter_num = vd.encounter_num
    and obs.patient_num = vd.patient_num)
when matched then update
 set vd.length_of_stay = obs.length_of_stay
         , vd.end_date = obs.discharge_date
;
-- 194,328 rows merged.

merge into "&&i2b2_data_schema".visit_dimension vd
using (
  -- how about discharge disposition?
  select obs.encounter_num, obs.patient_num
       , max(obs.start_date) discharge_date
  from "&&i2b2_data_schema".observation_fact obs
  join (
    select distinct concept_cd
    from "&&i2b2_data_schema".concept_dimension
      -- TODO: map to PCORnet path
    where concept_path like '\i2b2\Visit Details\Discharge Disposition Codes\%'
    group by concept_cd
  ) cd on cd.concept_cd = obs.concept_cd
  group by encounter_num, patient_num
) obs
on (  obs.encounter_num = vd.encounter_num
    and obs.patient_num = vd.patient_num)
when matched then update
 set vd.length_of_stay = obs.discharge_date - vd.start_date
         , vd.end_date = obs.discharge_date
;
-- 3,879,931 rows merged.


/* -- Manually update encounter (i2p-transform should fill in the column)
merge into encounter pe
using "&&i2b2_data_schema".visit_dimension vd
   on ( pe.encounterid = vd.encounter_num
        and vd.end_date is not null)
when matched then update set pe.discharge_date = vd.end_date;
-- 165,292 rows merged.
*/


/* TODO: get encounter type from source system? */
/* TODO: Consider moving to cdm_transform_tests.sql
select count(*), sourcesystem_cd
from nightherondata.visit_dimension
group by sourcesystem_cd;
*/

/* TODO: get encounter type from place of service? */
/* TODO: Consider moving to cdm_transform_tests.sql
select obs.encounter_num, obs.patient_num, obs.start_date, obs.end_date
     , con.concept_cd, con.name_char place_of_service
     , obs.sourcesystem_cd
from "&&i2b2_data_schema".observation_fact obs
join "&&i2b2_data_schema".concept_dimension con on con.concept_cd = obs.concept_cd
join "&&i2b2_data_schema".visit_dimension v on v.encounter_num = obs.encounter_num
where con.concept_path like '\i2b2\Visit Details\Place of Service (IDX)\%'
and v.inout_cd = 'OT'
;
*/

/* Update the visit dimension to populate the inout_cd with the appropriate 
PCORNet encounter type codes based on data in the observation_fact.

The pcornet_mapping table contains columns that translate the PCORNet paths to
local paths.

Example:
pcori_path,local_path
\PCORI\ENCOUNTER\ENC_TYPE\AV\,\i2b2\Visit Details\ENC_TYPE\AV\
*/ 
select * from pcornet_mapping pm where 1=0;

whenever sqlerror continue;
drop table enc_type;
whenever sqlerror exit;

/* explore encounter mapping: where do our encounters come from? */
select count(*), count(distinct encounter_num), encounter_ide_source
from "&&i2b2_data_schema".encounter_mapping
group by encounter_ide_source
order by 1 desc
;

select min(encounter_num), encounter_ide_source
from "&&i2b2_data_schema".encounter_mapping
group by encounter_ide_source
order by 1
;


create table enc_type as
with
enc_type_codes as (
  select 
    pm.pcori_path, replace(substr(pm.pcori_path, instr(pm.pcori_path, '\', -2)), '\', '') pcori_code, pm.local_path, cd.concept_cd
  from pcornet_mapping pm 
  join "&&i2b2_data_schema".concept_dimension cd on cd.concept_path like pm.local_path || '%'
  where pm.pcori_path like '\PCORI\ENCOUNTER\ENC_TYPE\%'
  )
-- TODO: Consider investigating why we have multiple encounter types for a single encounter
select obs.encounter_num, max(et.pcori_code) pcori_code
from "&&i2b2_data_schema".observation_fact obs
join enc_type_codes et on et.concept_cd = obs.concept_cd
group by obs.encounter_num
;

create index enc_type_codes_encnum_idx on enc_type(encounter_num);

update "&&i2b2_data_schema".visit_dimension vd
set inout_cd = coalesce((
  select et.pcori_code from enc_type et
  where vd.encounter_num = et.encounter_num
  ), 'UN');

-- Make sure the update went as expected
select case when count(*) > 0 then 1/0 else 1 end inout_cd_update_match from (
  select * from (
    select vd.inout_cd vd_code, et.pcori_code et_code
    from "&&i2b2_data_schema".visit_dimension vd
    left join enc_type et on et.encounter_num = vd.encounter_num
    )
  where vd_code != vd_code and not (vd_code = 'UN' and vd_code is null)
  );


/* Discharge disposition
Valid values are:

A=Discharged alive
E=Expired
NI=No information
UN=Unknown
OT=Other

So, if there are multiple discharge dispositions per encounter (as we've observed
in test data at least) prioritize and pick one.
*/
whenever sqlerror continue;
alter table "&&i2b2_data_schema".visit_dimension add (
  discharge_disposition  VARCHAR2(4 BYTE)
  );
drop table enc_disp_facts;
drop table discharge_disp;
whenever sqlerror exit;

-- Two steps (tables) due to Oracle performance/picking bad plans when it's all one query
create table enc_disp_facts as
with
enc_type_codes as (
  select 
    pm.pcori_path, replace(substr(pm.pcori_path, instr(pm.pcori_path, '\', -2)), '\', '') pcori_code, pm.local_path, cd.concept_cd
  from pcornet_mapping pm 
  join "&&i2b2_data_schema".concept_dimension cd on cd.concept_path like pm.local_path || '%'
  where pm.pcori_path like '\PCORI\ENCOUNTER\DISCHARGE_DISPOSITION\%'
  )
select obs.encounter_num, et.pcori_code
from "&&i2b2_data_schema".observation_fact obs
join enc_type_codes et on et.concept_cd = obs.concept_cd;

create table discharge_disp as
with
ranks as (
  select 
    encounter_num, pcori_code,
    -- Prioritize disposition
    decode(pcori_code, 'E', 0, 'A', 1, 'OT', 2, 'UN', 3, 'NI', 4, 99) pc_rank 
    from enc_disp_facts
  ),
priority_rank as (
  -- TODO: Consider figuring out why we have multiple discharge dispositions per encounter.
  select min(pc_rank) pc_rank, encounter_num 
  from ranks
  group by encounter_num
  ) 
select distinct
  ranks.encounter_num, coalesce(ranks.pcori_code, 'NI') pcori_code 
from priority_rank pr
join ranks on pr.encounter_num = ranks.encounter_num and pr.pc_rank = ranks.pc_rank
;

create index discharge_disp_encnum_idx on discharge_disp(encounter_num);

update "&&i2b2_data_schema".visit_dimension vd
set discharge_disposition = coalesce((
  select dd.pcori_code from discharge_disp dd
  where vd.encounter_num = dd.encounter_num
  ), 'UN');
