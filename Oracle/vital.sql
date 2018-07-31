/** vital - create and populate the vital table.
*/
insert into cdm_status (task, start_time) select 'vital', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE vital');
END;
/
CREATE TABLE vital (
	VITALID varchar(19)  primary key,
	PATID varchar(50) NULL,
	ENCOUNTERID varchar(50) NULL,
	MEASURE_DATE date NULL,
	MEASURE_TIME varchar(5) NULL,
	VITAL_SOURCE varchar(2) NULL,
	HT number(18, 0) NULL, --8, 0
	WT number(18, 0) NULL, --8, 0
	DIASTOLIC number(18, 0) NULL,--4, 0
	SYSTOLIC number(18, 0) NULL, --4, 0
	ORIGINAL_BMI number(18,0) NULL,--8, 0
	BP_POSITION varchar(2) NULL,
	SMOKING varchar (2),
	TOBACCO varchar (2),
	TOBACCO_TYPE varchar (2),
	RAW_VITAL_SOURCE varchar(50) NULL,
	RAW_HT varchar(50) NULL,
	RAW_WT varchar(50) NULL,
	RAW_DIASTOLIC varchar(50) NULL,
	RAW_SYSTOLIC varchar(50) NULL,
	RAW_BP_POSITION varchar(50) NULL,
	RAW_SMOKING varchar (50),
	Raw_TOBACCO varchar (50),
	Raw_TOBACCO_TYPE varchar (50)
)
/

BEGIN
PMN_DROPSQL('DROP SEQUENCE vital_seq');
END;
/
create sequence  vital_seq
/

create or replace trigger vital_trg
before insert on vital
for each row
begin
  select vital_seq.nextval into :new.VITALID from dual;
end;
/
create or replace procedure PCORNetVital as
begin

PMN_DROPSQL('drop index vital_idx');

execute immediate 'truncate table vital';

-- jgk: I took out admit_date - it doesn't appear in the scheme. Now in SQLServer format - date, substring, name on inner select, no nested with. Added modifiers and now use only pathnames, not codes.
insert into vital(patid, encounterid, measure_date, measure_time,vital_source,ht, wt, diastolic, systolic, original_bmi, bp_position,smoking,tobacco,tobacco_type)
select patid, encounterid, to_date(measure_date,'rrrr-mm-dd') measure_date, measure_time,vital_source,ht, wt, diastolic, systolic, original_bmi, bp_position,smoking,tobacco,
case when tobacco in ('02','03','04') then -- no tobacco
    case when smoking in ('03','04') then '04' -- no smoking
        when smoking in ('01','02','07','08') then '01' -- smoking
        else 'NI' end
 when tobacco='01' then
    case when smoking in ('03','04') then '02' -- no smoking
        when smoking in ('01','02','07','08') then '03' -- smoking
        else '05' end
 else 'NI' end tobacco_type
from
(select patid, encounterid, measure_date, measure_time, NVL(max(vital_source),'HC') vital_source, -- jgk: not in the spec, so I took it out  admit_date,
max(ht) ht, max(wt) wt, max(diastolic) diastolic, max(systolic) systolic,
max(original_bmi) original_bmi, NVL(max(bp_position),'NI') bp_position,
NVL(NVL(max(smoking),max(unk_tobacco)),'NI') smoking,
NVL(NVL(max(tobacco),max(unk_tobacco)),'NI') tobacco
from (
  select vit.patid, vit.encounterid, vit.measure_date, vit.measure_time
    , case when vit.pcori_code like '\PCORI\VITAL\HT%' then vit.nval_num else null end ht
    , case when vit.pcori_code like '\PCORI\VITAL\WT%' then vit.nval_num else null end wt
    , case when vit.pcori_code like '\PCORI\VITAL\BP\DIASTOLIC%' then vit.nval_num else null end diastolic
    , case when vit.pcori_code like '\PCORI\VITAL\BP\SYSTOLIC%' then vit.nval_num else null end systolic
    , case when vit.pcori_code like '\PCORI\VITAL\ORIGINAL_BMI%' then vit.nval_num else null end original_bmi
    , case when vit.pcori_code like '\PCORI_MOD\BP_POSITION\%' then SUBSTR(vit.pcori_code,LENGTH(vit.pcori_code)-2,2) else null end bp_position
    , case when vit.pcori_code like '\PCORI_MOD\VITAL_SOURCE\%' then SUBSTR(vit.pcori_code,LENGTH(vit.pcori_code)-2,2) else null end vital_source
    , case when vit.pcori_code like '\PCORI\VITAL\TOBACCO\02\%' then vit.pcori_basecode else null end tobacco
    , case when vit.pcori_code like '\PCORI\VITAL\TOBACCO\SMOKING\%' then vit.pcori_basecode else null end smoking
    , case when vit.pcori_code like '\PCORI\VITAL\TOBACCO\__\%' then vit.pcori_basecode else null end unk_tobacco
    , enc.admit_date
  from demographic pd
  left join (
    select
      obs.patient_num patid, obs.encounter_num encounterid,
	to_char(obs.start_Date,'YYYY-MM-DD') measure_date,
	to_char(obs.start_Date,'HH24:MI') measure_time,
      nval_num, pcori_basecode, codes.pcori_code
    from i2b2fact obs
    inner join (select c_basecode concept_cd, c_fullname pcori_code, pcori_basecode
      from (
        select '\PCORI\VITAL\BP\DIASTOLIC\' concept_path  FROM DUAL
        union all
        select '\PCORI\VITAL\BP\SYSTOLIC\' concept_path  FROM DUAL
        union all
        select '\PCORI\VITAL\HT\' concept_path FROM DUAL
        union all
        select '\PCORI\VITAL\WT\' concept_path FROM DUAL
        union all
        select '\PCORI\VITAL\ORIGINAL_BMI\' concept_path FROM DUAL
        union all
        select '\PCORI_MOD\BP_POSITION\' concept_path FROM DUAL
        union all
        select '\PCORI_MOD\VITAL_SOURCE\' concept_path FROM DUAL
        union all
        select '\PCORI\VITAL\TOBACCO\' concept_path FROM DUAL
        ) bp, pcornet_vital pm
      where pm.c_fullname like bp.concept_path || '%'
      ) codes on codes.concept_cd = obs.concept_cd
    ) vit on vit.patid = pd.patid
  join encounter enc on enc.encounterid = vit.encounterid
  ) x
where ht is not null
  or wt is not null
  or diastolic is not null
  or systolic is not null
  or original_bmi is not null
  or bp_position is not null
  or vital_source is not null
  or smoking is not null
  or tobacco is not null
group by patid, encounterid, measure_date, measure_time, admit_date) y;

execute immediate 'create index vital_idx on vital (PATID)';
GATHER_TABLE_STATS('VITAL');

end PCORNetVital;
/
BEGIN
PCORNetVital();
END;
/
update cdm_status
set end_time = sysdate, records = (select count(*) from vital)
where task = 'vital'
/
select records from cdm_status where task = 'vital'
