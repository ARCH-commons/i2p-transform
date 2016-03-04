/* Update the patient dimension to include column ETHNICITY_CD and populate it
with the appropriate PCORNet codes based on data in the observation_fact.

The pcornet_mapping table contains columns that translate the PCORNet paths to
local paths.

Example:
pcori_path,local_path
\PCORI\DEMOGRAPHIC\HISPANIC\Y\,\i2b2\Demographics\Ethnicity\Hispanic\

For ethnicity, it's assumed that the last field in the path is the PCORNet code.
In the example above, it's "Y".
*/ 
select * from pcornet_mapping pm where 1=0;

whenever sqlerror continue;
alter table "&&i2b2_data_schema".patient_dimension add (
  ETHNICITY_CD  VARCHAR2(50 BYTE)
  );
drop table hispanic_patients;
whenever sqlerror exit;

create table hispanic_patients as
with
hispanic_codes as (
  select 
    pm.pcori_path, replace(substr(pm.pcori_path, instr(pm.pcori_path, '\', -2)), '\', '') pcori_code, pm.local_path, cd.concept_cd
  from pcornet_mapping pm 
  join "&&i2b2_data_schema".concept_dimension cd on cd.concept_path like pm.local_path || '%'
  where pm.pcori_path like '\PCORI\DEMOGRAPHIC\HISPANIC\%'
  )
-- Sometimes, we have more than one ethnicity fact for a given patient
select obs.patient_num, max(hc.pcori_code) pcori_code
from "&&i2b2_data_schema".observation_fact obs
join hispanic_codes hc on hc.concept_cd = obs.concept_cd
group by obs.patient_num
;

update "&&i2b2_data_schema".patient_dimension pd
set ethnicity_cd = coalesce((
  select hp.pcori_code from hispanic_patients hp
  where pd.patient_num = hp.patient_num
  ), 'NI');

-- Make sure the update went as expected
select case when count(*) > 0 then 1/0 else 1 end ethnicity_update_match from (
  select * from (
    select pd.ethnicity_cd pd_code, hp.pcori_code hp_code
    from "&&i2b2_data_schema".patient_dimension pd
    left join hispanic_patients hp on hp.patient_num = pd.patient_num
    )
  where pd_code != hp_code and not (pd_code = 'NI' and hp_code is null)
  );
