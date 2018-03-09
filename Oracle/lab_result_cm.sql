/** lab_result_cm - create and populate the lab_result_cm table.
*/

select encounterid from encounter where 'dep' = 'encounter.sql'
/
BEGIN
PMN_DROPSQL('DROP TABLE lab_result_cm');
END;
/
CREATE TABLE lab_result_cm(
	LAB_RESULT_CM_ID varchar(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID varchar(50) NULL,
	LAB_NAME varchar(10) NULL,
	SPECIMEN_SOURCE varchar(10) NULL,
	LAB_LOINC varchar(10) NULL,
	PRIORITY varchar(2) NULL,
	RESULT_LOC varchar(2) NULL,
	LAB_PX varchar(11) NULL,
	LAB_PX_TYPE varchar(2) NULL,
	LAB_ORDER_DATE date NULL,
	SPECIMEN_DATE date NULL,
	SPECIMEN_TIME varchar(5) NULL,
	RESULT_DATE date NULL,
	RESULT_TIME varchar(5) NULL,
	RESULT_QUAL varchar(12) NULL,
	RESULT_NUM number (15,8) NULL,
	RESULT_MODIFIER varchar(2) NULL,
	RESULT_UNIT varchar(11) NULL,
	NORM_RANGE_LOW varchar(10) NULL,
	NORM_MODIFIER_LOW varchar(2) NULL,
	NORM_RANGE_HIGH varchar(10) NULL,
	NORM_MODIFIER_HIGH varchar(2) NULL,
	ABN_IND varchar(2) NULL,
	RAW_LAB_NAME varchar(50) NULL,
	RAW_LAB_CODE varchar(50) NULL,
	RAW_PANEL varchar(50) NULL,
	RAW_RESULT varchar(50) NULL,
	RAW_UNIT varchar(50) NULL,
	RAW_ORDER_DEPT varchar(50) NULL,
	RAW_FACILITY_CODE varchar(50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP SEQUENCE lab_result_cm_seq');
END;
/
create sequence  lab_result_cm_seq
/

create or replace trigger lab_result_cm_trg
before insert on lab_result_cm
for each row
begin
  select lab_result_cm_seq.nextval into :new.LAB_RESULT_CM_ID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE priority');
END;
/

CREATE TABLE PRIORITY  (
	PATIENT_NUM  	NUMBER(38) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	START_DATE   	DATE NOT NULL,
	PRIORITY     	VARCHAR2(50) NULL
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE location');
END;
/

CREATE TABLE LOCATION  (
	PATIENT_NUM  	NUMBER(38) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	START_DATE   	DATE NOT NULL,
	RESULT_LOC   	VARCHAR2(50) NULL
	)
/
create or replace procedure PCORNetLabResultCM as
begin

PMN_DROPSQL('drop index lab_result_cm_idx');
PMN_DROPSQL('drop index priority_idx');
PMN_DROPSQL('drop index location_idx');

execute immediate 'truncate table priority';
execute immediate 'truncate table location';
execute immediate 'truncate table lab_result_cm';

insert into priority
select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, lsource.pcori_basecode PRIORITY
from i2b2fact
inner join encounter enc on enc.patid = i2b2fact.patient_num and enc.encounterid = i2b2fact.encounter_Num
inner join pcornet_lab lsource on i2b2fact.modifier_cd =lsource.c_basecode
where c_fullname LIKE '\PCORI_MOD\PRIORITY\%';

execute immediate 'create index priority_idx on priority (patient_num, encounter_num, provider_id, concept_cd, start_date)';
GATHER_TABLE_STATS('PRIORITY');

insert into location
select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, lsource.pcori_basecode  RESULT_LOC
from i2b2fact
inner join encounter enc on enc.patid = i2b2fact.patient_num and enc.encounterid = i2b2fact.encounter_Num
inner join pcornet_lab lsource on i2b2fact.modifier_cd =lsource.c_basecode
where c_fullname LIKE '\PCORI_MOD\RESULT_LOC\%';

execute immediate 'create index location_idx on location (patient_num, encounter_num, provider_id, concept_cd, start_date)';
GATHER_TABLE_STATS('LOCATION');

INSERT INTO lab_result_cm
      (PATID
      ,ENCOUNTERID
      ,LAB_NAME
      ,SPECIMEN_SOURCE
      ,LAB_LOINC
      ,PRIORITY
      ,RESULT_LOC
      ,LAB_PX
      ,LAB_PX_TYPE
      ,LAB_ORDER_DATE
      ,SPECIMEN_DATE
      ,SPECIMEN_TIME
      ,RESULT_DATE
      ,RESULT_TIME
      ,RESULT_QUAL
      ,RESULT_NUM
      ,RESULT_MODIFIER
      ,RESULT_UNIT
      ,NORM_RANGE_LOW
      ,NORM_MODIFIER_LOW
      ,NORM_RANGE_HIGH
      ,NORM_MODIFIER_HIGH
      ,ABN_IND
      ,RAW_LAB_NAME
      ,RAW_LAB_CODE
      ,RAW_PANEL
      ,RAW_RESULT
      ,RAW_UNIT
      ,RAW_ORDER_DEPT
      ,RAW_FACILITY_CODE)

SELECT DISTINCT  M.patient_num patid,
M.encounter_num encounterid,
CASE WHEN ont_parent.C_BASECODE LIKE 'LAB_NAME%' then SUBSTR (ont_parent.c_basecode,10, 10) ELSE 'UN' END LAB_NAME,
CASE WHEN lab.pcori_specimen_source like '%or SR_PLS' THEN 'SR_PLS' WHEN lab.pcori_specimen_source is null then 'NI' ELSE lab.pcori_specimen_source END specimen_source, -- (Better way would be to fix the column in the ontology but this will work)
NVL(lab.pcori_basecode, 'NI') LAB_LOINC,
NVL(p.PRIORITY,'NI') PRIORITY,
NVL(l.RESULT_LOC,'NI') RESULT_LOC,
NVL(lab.pcori_basecode, 'NI') LAB_PX,
'LC'  LAB_PX_TYPE,
m.start_date LAB_ORDER_DATE,
m.start_date SPECIMEN_DATE,
to_char(m.start_date,'HH24:MI')  SPECIMEN_TIME,
m.end_date RESULT_DATE,
to_char(m.end_date,'HH24:MI') RESULT_TIME,
--CASE WHEN m.ValType_Cd='T' THEN NVL(nullif(m.TVal_Char,''),'NI') ELSE 'NI' END RESULT_QUAL, -- TODO: Should be a standardized value
'NI' RESULT_QUAL, -- Local fix for KUMC (temp)
CASE WHEN m.ValType_Cd='N' THEN m.NVAL_NUM ELSE null END RESULT_NUM,
CASE WHEN m.ValType_Cd='N' THEN (CASE NVL(nullif(m.TVal_Char,''),'NI') WHEN 'E' THEN 'EQ' WHEN 'NE' THEN 'OT' WHEN 'L' THEN 'LT' WHEN 'LE' THEN 'LE' WHEN 'G' THEN 'GT' WHEN 'GE' THEN 'GE' ELSE 'NI' END)  ELSE 'TX' END RESULT_MODIFIER,
--NVL(m.Units_CD,'NI') RESULT_UNIT, -- TODO: Should be standardized units
CASE
  WHEN INSTR(m.Units_CD, '%') > 0 THEN 'PERCENT'
  WHEN m.Units_CD IS NULL THEN NVL(m.Units_CD,'NI')
  when length(m.Units_CD) > 11 then substr(m.Units_CD, 1, 11)
  ELSE TRIM(REPLACE(UPPER(m.Units_CD), '(CALC)', ''))
end RESULT_UNIT, -- Local fix for KUMC
norm.ref_lo NORM_RANGE_LOW,
case
  when norm.ref_lo is not null and norm.ref_hi is not null then 'EQ'
  when norm.ref_lo is not null and norm.ref_hi is null then 'GE'
  when norm.ref_lo is null and norm.ref_hi is not null then 'NO'
else 'NI'
end NORM_MODIFIER_LOW,
norm.ref_hi NORM_RANGE_HIGH,
case
  when norm.ref_lo is not null and norm.ref_hi is not null then 'EQ'
  when norm.ref_lo is not null and norm.ref_hi is null then 'NO'
  when norm.ref_lo is null and norm.ref_hi is not null then 'LE'
  else 'NI'
end NORM_MODIFIER_HIGH,
CASE NVL(nullif(m.VALUEFLAG_CD,''),'NI') WHEN 'H' THEN 'AH' WHEN 'L' THEN 'AL' WHEN 'A' THEN 'AB' ELSE 'NI' END ABN_IND,
NULL RAW_LAB_NAME,
NULL RAW_LAB_CODE,
NULL RAW_PANEL,
--CASE WHEN m.ValType_Cd='T' THEN m.TVal_Char ELSE to_char(m.NVal_Num) END RAW_RESULT,
CASE WHEN m.ValType_Cd='T' THEN substr(m.TVal_Char, 1, 50) ELSE to_char(m.NVal_Num) END RAW_RESULT, -- Local fix for KUMC
NULL RAW_UNIT,
NULL RAW_ORDER_DEPT,
m.concept_cd RAW_FACILITY_CODE

FROM i2b2fact M
inner join encounter enc on enc.patid = m.patient_num and enc.encounterid = m.encounter_Num -- Constraint to selected encounters
inner join demographic demo on demo.patid=m.patient_num
inner join pcornet_lab lab on lab.c_basecode  = M.concept_cd and lab.c_fullname like '\PCORI\LAB_RESULT_CM\%'
inner JOIN pcornet_lab ont_parent on lab.c_path=ont_parent.c_fullname

LEFT OUTER JOIN priority p

ON  M.patient_num=p.patient_num
and M.encounter_num=p.encounter_num
and M.provider_id=p.provider_id
and M.concept_cd=p.concept_Cd
and M.start_date=p.start_Date

LEFT OUTER JOIN location l

ON  M.patient_num=l.patient_num
and M.encounter_num=l.encounter_num
and M.provider_id=l.provider_id
and M.concept_cd=l.concept_Cd
and M.start_date=l.start_Date

LEFT OUTER JOIN labnormal norm
  on m.concept_cd=norm.concept_cd
  and demo.sex=norm.sex
  and (m.start_date - demo.birth_date) > norm.age_lower
  and (m.start_date - demo.birth_date) <= norm.age_upper

WHERE m.ValType_Cd in ('N','T')
and m.MODIFIER_CD='@'
and (m.nval_num is null or m.nval_num<=9999999) -- exclude lengths that exceed the spec
;

execute immediate 'create index lab_result_cm_idx on lab_result_cm (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('LAB_RESULT_CM');

END PCORNetLabResultCM;
/
BEGIN
PCORNetLabResultCM();
END;
/
insert into cdm_status (status, last_update, records) select 'lab_result_cm', sysdate, count(*) from lab_result_cm
/
select 1 from cdm_status where status = 'lab_result_cm'
--SELECT count(LAB_RESULT_CM_ID) from lab_result_cm where rownum = 1