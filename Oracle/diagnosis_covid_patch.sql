/**
XXII Codes for special purposes  
  U00-U49 Provisional assignment of new diseases of uncertain etiology or emergency use  
    U04 Severe acute respiratory syndrome [SARS]  
    U07 Emergency use of U07  
      U07.0 Vaping-related disorder  
      U07.1 COVID-19, virus identified  
      U07.2 COVID-19, virus not identified
https://icd.who.int/browse10/2019/en#/U07.1
*/

set echo on;
/**  Mix PCORNet diagnosis ICD10 root with WHO Emergency codes for COVID-19 */
WHENEVER SQLERROR CONTINUE;
drop table pcori_who_covid;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

create table pcori_who_covid as
with icd10_dx_root as (
select *
from nightherondata.pcornet_diag
where c_fullname = '\PCORI\DIAGNOSIS\10\A20098492\'
order by c_fullname
),
who_covid_2 as (
-- select 1 as c_hlevel, 'XXII Codes for special purposes' as c_name from dual union all
select 3 as c_hlevel, 'U00-U49 Provisional assignment of new diseases of uncertain etiology or emergency use' as c_name
     , 'ICD10:U00-U49' as c_basecode
     ,  parent.c_fullname || 'ICD10:U00-U49\' as c_fullname
     , null pcori_basecode
from icd10_dx_root parent
),
who_covid_3 as (
select 4 as c_hlevel, 'U07 Emergency use of U07' as c_name, 'ICD10:U07' as c_basecode
     , parent.c_fullname || 'ICD10:U07\' as c_fullname
     , null pcori_basecode
from who_covid_2 parent
),
who_covid_4 as (
select 5 as c_hlevel, 'U07.1 COVID-19, virus identified' as c_name, 'ICD10:U07.1' as c_basecode from dual union all
select 5 as c_hlevel, 'U07.2 COVID-19, virus not identified' as c_name, 'ICD10:U07.2' as c_basecode from dual
)
, ea as (
select * from who_covid_2 union all
select * from who_covid_3 union all
select leaf.*, parent.c_fullname || leaf.c_basecode || '\' as c_fullname
     , replace(leaf.c_basecode, 'ICD10:', '') as pcori_basecode
from who_covid_4 leaf cross join who_covid_3 parent
)
select ea.c_hlevel
     , ea.c_fullname
     , ea.c_name
     , m.c_synonym_cd
     , case when ea.c_basecode like '%.%' then 'LA' else 'FA' end as c_visualattributes -- @@Always folder
     , m.c_totalnum
     , ea.c_basecode
     , m.c_metadataxml
     , m.c_facttablecolumn
     , m.c_tablename
     , m.c_columnname
     , m.c_columndatatype
     , m.c_operator
     , m.c_dimcode
     , m.c_comment
     , 'TODO' c_tooltip
     , m.m_applied_path
     , sysdate update_date
     , m.download_date
     , sysdate import_date
     , 'heron@kumc.edu' sourcesystem_cd
     , m.valuetype_cd
     , m.m_exclusion_cd
     , m.c_path
     , m.c_symbol
     , ea.pcori_basecode
from ea
cross join icd10_dx_root m
;


/** O2 / Epic / IMO terms for covid */
WHENEVER SQLERROR CONTINUE;
drop table covid_dx_id_terms;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

create table covid_dx_id_terms as
with ea as (
select *
from clarity.clarity_edg
where current_icd10_list like '%U07.1%'
)
select m.c_hlevel + 1 as c_hlevel
     , m.c_fullname || 'DX_ID ' || ea.dx_id || '\' as c_fullname
     , ea.dx_name as c_name
     , m.c_synonym_cd
     , 'LA' as c_visualattributes
     , m.c_totalnum
     , 'KUH|DX_ID:' || ea.dx_id as c_basecode
     , m.c_metadataxml
     , m.c_facttablecolumn
     , m.c_tablename
     , m.c_columnname
     , m.c_columndatatype
     , m.c_operator
     , m.c_dimcode
     , m.c_comment
     , 'TODO' c_tooltip
     , m.m_applied_path
     , sysdate update_date
     , m.download_date
     , sysdate import_date
     , 'heron@kumc.edu' sourcesystem_cd
     , m.valuetype_cd
     , m.m_exclusion_cd
     , m.c_path
     , m.c_symbol
     , m.pcori_basecode
from pcori_who_covid m
cross join ea
where m.pcori_basecode = 'U07.1'
;


insert into nightherondata.pcornet_diag
select * from pcori_who_covid
union all
select * from COVID_DX_ID_TERMS
;
commit;


/** part 2 */
-- darn; didn't work:
--select count(*) from diagnosis where dx_type=10 and dx = 'U07.1';

-- do have metadata connecting DX_ID to U07.1? yes...
WHENEVER SQLERROR CONTINUE;
drop table covid_meta purge;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

create table covid_meta as
  select distinct c_basecode, pcori_basecode icd10_code
  from nightherondata.pcornet_diag diag
  where diag.c_fullname like '\PCORI\DIAGNOSIS\10%'
  and pcori_basecode = 'U07.1';


-- let's collect the U07.1 facts...
WHENEVER SQLERROR CONTINUE;
drop table covid_dx_facts purge;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

create table covid_dx_facts as
select f.*
from i2b2fact f
where f.concept_cd in (
select c_basecode from covid_meta
);

WHENEVER SQLERROR CONTINUE;
drop table diagnosis_c19_no_orphan_enc purge;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

create table diagnosis_c19_no_orphan_enc as select * from diagnosis where 1=0;

insert into diagnosis_c19_no_orphan_enc
SELECT
    diagnosis_seq.nextval diagnosisid,
    f.patient_num patid,
    f.encounter_num encounterid,
    enc.enc_type, -- , 'UN') enc_type, -- ISSUE: encounter mis-matches
    enc.admit_date, -- , trunc(f.start_date)), -- ISSUE
    trunc(f.start_date) dx_date,
    case when f.provider_id = '@' then null else f.provider_id end providerid,
    'U07.1' dx,
    '10' dx_type,
    case
    WHEN enc_type in ('AV', 'OA') THEN 'FI' -- CDM 5.1: Ambulatory encounters would generally be expected to have a source of "Final."
    when raw_dx_source like '%:ADMIT%' then 'AD'
    when raw_dx_source like '%DX|BILL:DC%' then 'DI' -- discharge
    when raw_dx_source like '%DX|BILL:POA%' then 'DI' -- from pcornet_diag modifers
    when raw_dx_source like '%DiagObs:PAT_ENC_DX%' then 'UN'
    when raw_dx_source like '%PROF:PRIMARY%' then 'FI' -- DX_SOURCE:FI	DX|PROF:PRIMARY
    when raw_dx_source like '%PROF:NONPRIMARY%' then 'FI' -- DX_SOURCE:FI	DX|PROF:PRIMARY
    when raw_dx_source like '%MEDICAL_HX%' then 'OT' -- medical history -> other
    when raw_dx_source like '%PROBLEM_LIST%' then 'OT' -- ISSUE: source in case of PROBLEM_LIST?
    -- ISSUE: else???
    end dx_source,
    case
    when raw_dx_source like '%DX|BILL:%' then 'BI' -- billing
    when raw_dx_source like '%DX|PROF:%' then 'BI' -- professional charges -> billing
    when raw_dx_source like '%:UHC_DIAGNOSIS%' then 'BI' -- from pcornet_diag
    when raw_dx_source like '%PAT_ENC_DX%' or raw_dx_source like '%PROBLEM_LIST%' then 'OD' -- Order/EHR
    when raw_dx_source like '%MEDICAL_HX%' then 'OT' -- medical history -> other
    end dx_origin,
    case
    when raw_dx_source like '%SECOND%' then 'S'
    when raw_dx_source like '%PRIMARY%' then 'P'
    end pdx,
    case when enc_type in ('EI', 'IP') then
      case
        when raw_dx_source like '%ADMIT%' then 'Y'
        when raw_dx_source like ':POA%' then 'Y'
        else 'NI'
      end
    end dx_poa,
    substr(f.raw_dx, 1, 50) raw_dx,
    null raw_dx_type,
    substr(f.raw_dx_source, 1, 50) raw_dx_source,
    null raw_origdx,
    null raw_pdx,
    null raw_dx_poa
FROM
    (
    select patient_num, encounter_num, start_date, provider_id
          , listagg(modifier_cd, ',') within group (order by modifier_cd) raw_dx_source
          , listagg(concept_cd, ',') within group (order by concept_cd) raw_dx
     from covid_dx_facts
     group by patient_num, encounter_num, start_date, provider_id 
    ) f
    join encounter enc on enc.encounterid = to_char(f.encounter_num)
-- where enc.enc_type in ('IP', 'EI')
-- where enc.encounterid is null
-- order by patid, admit_date, dx_date
;
commit;


insert /*+ APPEND */ into diagnosis
select * from diagnosis_c19_no_orphan_enc
;
commit;


-- TODO
-- from https://github.com/kumc-bmi/i2p-transform/blob/master/Oracle/pcornet_loader.sql#L12
merge into diagnosis d
using encounter e
     on (d.encounterid = e.encounterid)
when matched then update set d.providerid = e.providerid;
-- from https://github.com/kumc-bmi/i2p-transform/blob/master/Oracle/pcornet_loader.sql#L107

update diagnosis
set pdx='NI'
where pdx='1';

commit;
