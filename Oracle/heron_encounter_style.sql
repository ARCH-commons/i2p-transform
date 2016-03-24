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


merge into "&&i2b2_data_schema".visit_dimension vd
using (
  with
  enc_type_codes as (
    select pm.pcori_path
         , replace(substr(pm.pcori_path, instr(pm.pcori_path, '\', -2)), '\', '') pcori_code
         , pm.local_path
         , cd.concept_cd
  from pcornet_mapping pm 
  join "&&i2b2_data_schema".concept_dimension cd on cd.concept_path like pm.local_path || '%'
  where pm.pcori_path like '\PCORI\ENCOUNTER\ENC_TYPE\%'
  )
-- Note: our patient-day style encounters etc. may result in
-- multiple encounter types for a single encounter.
select obs.encounter_num, max(et.pcori_code) pcori_code
from "&&i2b2_data_schema".observation_fact obs
join enc_type_codes et on et.concept_cd = obs.concept_cd
group by obs.encounter_num
) et on ( vd.encounter_num = et.encounter_num )
when matched then update
set inout_cd = et.pcori_code;
-- 11,085,911 rows merged.

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


  /* Discharge status
There may be multiple per encounter - rank and pick one.
*/
whenever sqlerror continue;
alter table "&&i2b2_data_schema".visit_dimension add (
  discharge_status  VARCHAR2(4 BYTE)
  );
drop table enc_status_facts;
drop table discharge_status;
whenever sqlerror exit;

-- Two steps (tables) due to Oracle performance/picking bad plans when it's all one query
create table enc_status_facts as
with
enc_status_codes as (
  select 
    pm.pcori_path, replace(substr(pm.pcori_path, instr(pm.pcori_path, '\', -2)), '\', '') pcori_code, pm.local_path, cd.concept_cd
  from pcornet_mapping pm 
  join "&&i2b2_data_schema".concept_dimension cd on cd.concept_path like pm.local_path || '%'
  where pm.pcori_path like '\PCORI\ENCOUNTER\DISCHARGE_STATUS\%'
  )
select obs.encounter_num, et.pcori_code
from "&&i2b2_data_schema".observation_fact obs
join enc_status_codes et on et.concept_cd = obs.concept_cd;

create table discharge_status as
with
ranks as (
  select 
    encounter_num, pcori_code,
    -- Prioritize status: First EX, Middle: <arbitrary>, Last OT, UN, NI
    decode(pcori_code, 'EX',0,'AF',1,'AL',2,'AM',3,'AW',4,'HH',5,'HO',6,'HS',7,
                       'IP',8,'NH',9,'RH',10,'RS',11,'SH',12,'SN',12,'OT',96,
                       'UN',97,'NI',98,99) pc_rank 
    from enc_status_facts
  ),
priority_rank as (
  -- TODO: Consider figuring out why we have multiple discharge status values per encounter.
  select min(pc_rank) pc_rank, encounter_num 
  from ranks
  group by encounter_num
  ) 
select distinct
  ranks.encounter_num, coalesce(ranks.pcori_code, 'NI') pcori_code 
from priority_rank pr
join ranks on pr.encounter_num = ranks.encounter_num and pr.pc_rank = ranks.pc_rank
;

create index discharge_status_encnum_idx on discharge_status(encounter_num);

update "&&i2b2_data_schema".visit_dimension vd
set discharge_status = coalesce((
  select ds.pcori_code from discharge_status ds
  where vd.encounter_num = ds.encounter_num
  ), 'UN');


/* Admitting source
There may be multiple per encounter - rank and pick one.
*/
whenever sqlerror continue;
alter table "&&i2b2_data_schema".visit_dimension add (
  admitting_source  VARCHAR2(4 BYTE)
  );
whenever sqlerror exit;

merge into "&&i2b2_data_schema".visit_dimension vd using (
  with codes as (
    select 
      pm.pcori_path, cd.concept_cd, cd.name_char, 
      replace(pe.pcori_basecode, 'ADMITTING_SOURCE:', '') pcori_code 
    from 
      pcornet_mapping pm 
    join "&&i2b2_data_schema".concept_dimension cd on cd.concept_path like pm.local_path || '%'
    join "&&i2b2_meta_schema".pcornet_enc pe on pe.c_fullname = pm.pcori_path
    where pm.pcori_path like '\PCORI\ENCOUNTER\ADMITTING_SOURCE\%'
    ),
  enc_as as (
    select obs.encounter_num, codes.pcori_code
    from "&&i2b2_data_schema".observation_fact obs
    join codes on codes.concept_cd = obs.concept_cd
    ),
  ranks as (
    select 
      encounter_num, pcori_code,
      -- Prioritize status: Last OT, UN, NI
      decode(pcori_code, 'AF',0,'AL',1,'AV',2,'ED',3,'HH',4,'HO',5,'HS',6,'IP',7,
                         'NH',8,'RH',9,'RS',10,'SN',11,'OT',12,'UN',13,'NI',14, 99) as_rank 
      from enc_as
    ),
  priority_rank as (
    select min(as_rank) as_rank, encounter_num 
    from ranks
    group by encounter_num
    ) 
  select distinct
    ranks.encounter_num, coalesce(ranks.pcori_code, 'NI') pcori_code 
  from priority_rank pr
  join ranks on pr.encounter_num = ranks.encounter_num and pr.as_rank = ranks.as_rank
  ) admt_enc on (admt_enc.encounter_num = vd.encounter_num)
when matched then update
set vd.admitting_source = admt_enc.pcori_code
;


/* -- Manually update encounter (i2p-transform should fill in the column)

select count(*) from encounter;
-- 14,665,341

merge into encounter pe
using "&&i2b2_data_schema".visit_dimension vd
   on ( pe.encounterid = vd.encounter_num
        and vd.end_date is not null)
when matched then update set pe.discharge_date = vd.end_date;
-- 165,292 rows merged.

merge into encounter pe
using "&&i2b2_data_schema".visit_dimension vd
   on ( pe.encounterid = vd.encounter_num
        and vd.inout_cd is not null)
when matched then update
 set pe.enc_type = vd.inout_cd;
 -- 14,665,341 rows merged.
*/
