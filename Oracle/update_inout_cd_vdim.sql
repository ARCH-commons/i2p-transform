/* Update the visit dimension to populate the inout_cd with the appropriate 
PCORNet encounter tyep codes based on data in the observation_fact.

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
