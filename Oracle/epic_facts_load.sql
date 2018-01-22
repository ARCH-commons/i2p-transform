/** epic_facts_load.sql - Load observations from Epic Clarity.

Copyright (c) 2012-2017 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

see also epic_dimensions_load.sql, 
  http://informatics.kumc.edu/work/wiki/HeronLoad
*/

/* Check for id repository, uploader service tables */
select * from NightHeronData.observation_fact where 1 = 0;
select * from NightHeronData.upload_status where 1 = 0;

/* We're wasting our time if an upload_status record isn't in place. */
select case count(*) when 1 then 1 else 1/0 end as upload_status_exists from (
select * from NightHeronData.upload_status up
where up.upload_id = :upload_id
);


create table observation_fact_&&upload_id as
select * from NightHeronData.observation_fact where 1=0;

insert /*+ append */ into observation_fact_&&upload_id (
  patient_num, encounter_num, sub_encounter,
  concept_cd,
  provider_id,
  start_date,
  modifier_cd,
  instance_num,
  valtype_cd,
  tval_char,
  nval_num,
  valueflag_cd,
  units_cd,
  end_date,
  location_cd,
  update_date,
  import_date, upload_id, download_date, sourcesystem_cd)
select pmap.patient_num,
  coalesce(emap.encounter_num, -abs(ora_hash(to_char(f.start_date, 'YYYYMMDD') || f.patient_ide))), -- TODO: pat_day func
  f.encounter_ide,
  f.concept_cd,
  f.provider_id,
  f.start_date,
  f.modifier_cd,
  f.instance_num,
  f.valtype_cd,
  f.tval_char,
  f.nval_num,
  f.valueflag_cd,
  f.units_cd,
  f.end_date,
  f.location_cd,
  f.update_date,
  sysdate, up.upload_id, :download_date, up.source_cd
from &&epic_fact_view f
          join NightHeronData.patient_mapping pmap
            on pmap.patient_ide = f.patient_ide
           and pmap.patient_ide_source = :pat_source_cd
           and pmap.patient_ide_status = 'A'  -- TODO: (select active from i2b2_status)
     left join NightHeronData.encounter_mapping emap
            on emap.encounter_ide = f.encounter_ide
           and emap.encounter_ide_source = :enc_source_cd
           and emap.encounter_ide_status = 'A'
   , NightHeronData.upload_status up
where up.upload_id = :upload_id
  and f.patient_ide between :pat_id_lo and :pat_id_hi
  &&log_fact_exceptions
;
commit
;


/* For this upload of data, check primary key constraints.
This could perhaps be factored out of all the load scripts into i2b2_facts_deid.sql,
at the cost of slowing down the select count(*) for summary stats, below.

TODO: check key constraint

alter table observation_fact_&&upload_id
  enable constraint observation_fact_pk
  -- TODO: log errors ... ? #2117
  ;
 */


select count(*)
from NightHeronData.upload_status
where transform_name = :task_id and load_status = 'OK';

