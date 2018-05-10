/** lab_result_cm - create and populate the lab_result_cm table.
*/
insert into cdm_status (task, start_time) select 'lab_result_cm', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE lab_result_cm');
END;
/

BEGIN
PMN_DROPSQL('drop table lab_result_key');
END;
/

BEGIN
PMN_DROPSQL('drop table lab_result_w_base');
END;
/

BEGIN
PMN_DROPSQL('drop table lab_result_w_parent');
END;
/

BEGIN
PMN_DROPSQL('drop table lab_result_w_norm');
END;
/

BEGIN
PMN_DROPSQL('DROP SEQUENCE lab_result_cm_seq');
END;
/

create sequence  lab_result_cm_seq cache 2000
/

/*
concept_cd between 'KUH|COMPONENT_ID:' and 'KUH|COMPONENT_ID:~'
works in place of c_fullname like '\PCORI\LAB_RESULT_CM\%'
because there are no leaves ('L_') outside this range.
See select below for example.

select c_name, c_basecode
from blueheronmetadata.heron_terms
where c_fullname like '\i2b2\Lab%'
and c_basecode not between 'KUH|COMPONENT_ID:' and 'KUH|COMPONENT_ID:~'
and c_visualattributes like 'L_'
;
*/
create table lab_result_key as
select lab_result_cm_seq.nextval LAB_RESULT_CM_ID
, patient_num PATID
, encounter_num ENCOUNTERID
, provider_id PROVIDERID
, start_date LAB_ORDER_DATE
, end_date RESULT_DATE
, nval_num RESULT_NUM
, tval_char RESULT_MODIFIER
, units_cd RESULT_UNIT
, valueflag_cd ABN_IND
, valtype_cd RAW_RESULT
, concept_cd RAW_FACILITY_CODE
from blueherondata.observation_fact m
join encounter enc on enc.patid = m.patient_num and enc.encounterid = m.encounter_Num
where concept_cd between 'KUH|COMPONENT_ID:' and 'KUH|COMPONENT_ID:~'
and modifier_cd in ('@');  -- exclude analyitics: Labs|Aggregate:Median, ...
and m.valtype_cd in ('N','T')
and (m.nval_num is null or m.nval_num<=9999999)
/

create table lab_result_w_base as
select lab.*
, pl.pcori_specimen_source SPECIMEN_SOURCE
, pl.pcori_basecode LAB_LOINC
, pl.c_path C_PATH
from lab_result_key lab
inner join
(
  select c_basecode, c_path, pcori_basecode, pcori_specimen_source
  from pcornet_lab
  where c_fullname like '\PCORI\LAB_RESULT_CM\%'
) pl
on lab.RAW_FACILITY_CODE = pl.c_basecode
/

create table lab_result_w_parent as
select lab.*
, parent.c_basecode LAB_NAME
from lab_result_w_base lab
inner join
(
  select c_fullname, c_basecode
  from pcornet_lab
) parent
on lab.C_PATH = parent.c_fullname;
/

create table lab_result_w_norm as
select lab.*
, ref_lo NORM_RANGE_LOW
, ref_hi NORM_RANGE_HIGH
from lab_result_w_parent lab
left join
(
  select patid, concept_cd, birth_date, age_lower, age_upper, ref_hi, ref_lo
  from labnormal
  join demographic
  on demographic.sex = labnormal.sex
) norm
on lab.PATID = norm.patid
and lab.RAW_FACILITY_CODE = norm.concept_cd
and (lab.LAB_ORDER_DATE - norm.birth_date) > norm.age_lower
and (lab.LAB_ORDER_DATE - norm.birth_date) <= norm.age_upper;
/

create table lab_result_cm as
select distinct cast(norm.LAB_RESULT_CM_ID as varchar(19)) LAB_RESULT_CM_ID
, cast(norm.PATID as varchar(50)) PATID
, cast(norm.ENCOUNTERID as varchar(50)) ENCOUNTERID
, case when norm.LAB_NAME like 'LAB_NAME%' then substr(norm.LAB_NAME, 10, 10) else 'UN' end LAB_NAME
, case when norm.SPECIMEN_SOURCE like '%or SR_PLS' then 'SR_PLS' when norm.SPECIMEN_SOURCE is null then 'NI' else norm.SPECIMEN_SOURCE end SPECIMEN_SOURCE
, nvl(norm.LAB_LOINC, 'NI') LAB_LOINC
, 'NI' PRIORITY
, 'NI' RESULT_LOC
, nvl(norm.LAB_LOINC, 'NI') LAB_PX
, 'LC'  LAB_PX_TYPE
, norm.LAB_ORDER_DATE LAB_ORDER_DATE
, norm.LAB_ORDER_DATE SPECIMEN_DATE
, to_char(norm.LAB_ORDER_DATE, 'HH24:MI')  SPECIMEN_TIME
, norm.RESULT_DATE
, to_char(norm.RESULT_DATE, 'HH24:MI') RESULT_TIME
, 'NI' RESULT_QUAL
, case when norm.RAW_RESULT = 'N' then norm.RESULT_NUM else null end RESULT_NUM
, case when norm.RAW_RESULT = 'N' then (case nvl(nullif(norm.RESULT_MODIFIER, ''),'NI') when 'E' then 'EQ' when 'NE' then 'OT' when 'L' then 'LT' when 'LE' then 'LE' when 'G' then 'GT' when 'GE' then 'GE' else 'NI' end)  else 'TX' end RESULT_MODIFIER
, case
  when instr(norm.RESULT_UNIT, '%') > 0 then 'PERCENT'
  when norm.RESULT_UNIT is null then nvl(norm.RESULT_UNIT, 'NI')
  when length(norm.RESULT_UNIT) > 11 then substr(norm.RESULT_UNIT, 1, 11)
  else trim(replace(upper(norm.RESULT_UNIT), '(CALC)', ''))
  end RESULT_UNIT
, norm.NORM_RANGE_LOW
, case
  when norm.NORM_RANGE_LOW is not null and norm.NORM_RANGE_HIGH is not null then 'EQ'
  when norm.NORM_RANGE_LOW is not null and norm.NORM_RANGE_HIGH is null then 'GE'
  when norm.NORM_RANGE_LOW is null and norm.NORM_RANGE_HIGH is not null then 'NO'
  else 'NI'
  end NORM_MODIFIER_LOW
, norm.NORM_RANGE_HIGH
, case nvl(nullif(norm.ABN_IND, ''), 'NI') when 'H' then 'AH' when 'L' then 'AL' when 'A' then 'AB' else 'NI' end ABN_IND
, cast(null as varchar(50)) RAW_LAB_NAME
, cast(null as varchar(50)) RAW_LAB_CODE
, cast(null as varchar(50)) RAW_PANEL
, case when norm.RAW_RESULT = 'T' then substr(norm.RESULT_MODIFIER, 1, 50) else to_char(norm.RESULT_NUM) end RAW_RESULT
, cast(null as varchar(50)) RAW_UNIT
, cast(null as varchar(50)) RAW_ORDER_DEPT
, norm.RAW_FACILITY_CODE RAW_FACILITY_CODE
from lab_result_w_norm norm;
/

create index lab_result_cm_idx on lab_result_cm (PATID, ENCOUNTERID)
/

BEGIN
GATHER_TABLE_STATS('LAB_RESULT_CM');
END;
/

update cdm_status
set end_time = sysdate, records = (select count(*) from lab_result_cm)
where task = 'lab_result_cm'
/

select 1 from cdm_status where task = 'lab_result_cm'