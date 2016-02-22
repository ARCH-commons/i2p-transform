-------------------------------------------------------------------------------------------
-- PCORNetLoader Script
-- Orignal MSSQL Verion Contributors: Jeff Klann, PhD; Aaron Abend; Arturo Torres
-- Translate to Oracle version: by Kun Wei(Wake Forest)
-- Version 0.6.2, bugfix release, 1/6/16 (create table and pcornetreport bugs)
-- Version 6.01, release to SCILHS, 10/15/15
-- Prescribing/dispensing bugfixes (untested) inserted by Jeff Klann 12/10/15
--
--
-- This is Orace Verion ELT v6 script to build PopMedNet database
-- Instructions:
--     (please see the original MSSQL version script.)
-------------------------------------------------------------------------------------------

--For undefining data/meta schema variables (SQLDeveloper at least)
--undef i2b2_data_schema;
--undef i2b2_meta_schema;

/* Create the loyalty cohort summary table if it doesn't exist.
TODO: Determine the side effects of doing so - I don't quite know what this 
table means or how having an empty one affects the results of the transform.
*/
whenever sqlerror continue;
create table "&&i2b2_data_schema".loyalty_cohort_patient_summary ( 
  patient_num number,
  filter_set number
);
whenever sqlerror exit;

create or replace PROCEDURE PMN_DROPSQL(sqlstring VARCHAR2) AS 
  BEGIN
      EXECUTE IMMEDIATE sqlstring;
  EXCEPTION
      WHEN OTHERS THEN NULL;
END PMN_DROPSQL;
/

create or replace FUNCTION PMN_IFEXISTS(objnamestr VARCHAR2, objtypestr VARCHAR2) RETURN BOOLEAN AS 
cnt NUMBER;
BEGIN
  SELECT COUNT(*)
   INTO cnt
    FROM USER_OBJECTS
  WHERE  upper(OBJECT_NAME) = upper(objnamestr)
         and upper(object_type) = upper(objtypestr);
  
  IF( cnt = 0 )
  THEN
    --dbms_output.put_line('NO!');
    return FALSE;  
  ELSE
   --dbms_output.put_line('YES!'); 
   return TRUE;
  END IF;

END PMN_IFEXISTS;
/


create or replace PROCEDURE PMN_Execuatesql(sqlstring VARCHAR2) AS 
BEGIN
  EXECUTE IMMEDIATE sqlstring;
  dbms_output.put_line(sqlstring);
END PMN_ExecuateSQL;
/






CREATE OR REPLACE SYNONYM I2B2FACT FOR "&&i2b2_data_schema".OBSERVATION_FACT
/


BEGIN
PMN_DROPSQL('DROP TABLE i2b2patient_list');
END;
/

CREATE table i2b2patient_list as 
select * from
(
select DISTINCT PATIENT_NUM from I2B2FACT where START_DATE > to_date('01-Jan-2010','dd-mon-rrrr')
) where ROWNUM<100000000
/

create or replace VIEW i2b2patient as select * from "&&i2b2_data_schema".PATIENT_DIMENSION where PATIENT_NUM in (select PATIENT_NUM from i2b2patient_list)
/

create or replace view i2b2visit as select * from "&&i2b2_data_schema".VISIT_DIMENSION where START_DATE >= to_date('01-Jan-2010','dd-mon-rrrr') and (END_DATE is NULL or END_DATE < CURRENT_DATE) and (START_DATE <CURRENT_DATE)
/


CREATE OR REPLACE SYNONYM pcornet_med FOR  "&&i2b2_meta_schema".pcornet_med
/

CREATE OR REPLACE SYNONYM pcornet_lab FOR  "&&i2b2_meta_schema".pcornet_lab
/

CREATE OR REPLACE SYNONYM pcornet_diag FOR  "&&i2b2_meta_schema".pcornet_diag
/

CREATE OR REPLACE SYNONYM pcornet_demo FOR  "&&i2b2_meta_schema".pcornet_demo
/

CREATE OR REPLACE SYNONYM pcornet_proc FOR  "&&i2b2_meta_schema".pcornet_proc
/

CREATE OR REPLACE SYNONYM pcornet_vital FOR  "&&i2b2_meta_schema".pcornet_vital
/

CREATE OR REPLACE SYNONYM pcornet_enc FOR  "&&i2b2_meta_schema".pcornet_enc
/

create or replace FUNCTION GETDATAMARTID RETURN VARCHAR2 IS 
BEGIN 
    RETURN 'WF';
END;
/

CREATE OR REPLACE FUNCTION GETDATAMARTNAME RETURN VARCHAR2 AS 
BEGIN 
    RETURN 'WakeForest';
END;
/

CREATE OR REPLACE FUNCTION GETDATAMARTPLATFORM RETURN VARCHAR2 AS 
BEGIN 
    RETURN '02'; -- 01 is MSSQL, 02 is Oracle
END;
/

create or replace view i2b2loyalty_patients as (select patient_num,to_date('01-Jul-2010','dd-mon-rrrr') period_start,to_date('01-Jul-2014','dd-mon-rrrr') period_end from "&&i2b2_data_schema".loyalty_cohort_patient_summary where BITAND(filter_set, 61511) = 61511 and patient_num in (select patient_num from i2b2patient))
/

BEGIN
PMN_DROPSQL('DROP TABLE pcornet_codelist');
END;
/

create table pcornet_codelist(codetype varchar2(20), code varchar2(50))
/
create or replace procedure pcornet_parsecode (codetype in varchar, codestring in varchar) as

tex varchar(2000);
pos number(9);
readstate char(1) ;
nextchar char(1) ;
val varchar(50);

begin

val:='';
readstate:='F';
pos:=0;
tex := codestring;
FOR pos IN 1..length(tex)
LOOP
--	dbms_output.put_line(val);
    	nextchar:=substr(tex,pos,1);
	if nextchar!=',' then
		if nextchar='''' then
			if readstate='F' then
				val:='';
				readstate:='T';
			else
				insert into pcornet_codelist values (codetype,val);
				val:='';
				readstate:='F'  ;
			end if;
		else
			if readstate='T' then
				val:= val || nextchar;
			end if;
		end if;
	end if;
END LOOP;

end pcornet_parsecode;
/



create or replace procedure pcornet_popcodelist as

codedata varchar(2000);
onecode varchar(20);
codetype varchar(20);

cursor getcodesql is
select 'RACE',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
union
select 'SEX',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
union
select 'HISPANIC',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC\Y%';


begin
open getcodesql;
LOOP 
	fetch getcodesql into codetype,codedata;
	EXIT WHEN getcodesql%NOTFOUND ;
 	pcornet_parsecode (codetype,codedata );
end loop;

close getcodesql ;
end pcornet_popcodelist;
/




BEGIN
PMN_DROPSQL('DROP TABLE pmnENROLLMENT');
END;
/

CREATE TABLE pmnENROLLMENT (
	PATID varchar(50) NOT NULL,
	ENR_START_DATE date NOT NULL,
	ENR_END_DATE date NULL,
	CHART varchar(1) NULL,
	BASIS varchar(1) NOT NULL,
	RAW_CHART varchar(50) NULL,
	RAW_BASIS varchar(50) NULL
)
/



BEGIN
PMN_DROPSQL('DROP TABLE pmnVITAL');
END;
/

CREATE TABLE pmnVITAL (
	VITALIID NUMBER(19)  primary key,
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
PMN_DROPSQL('DROP SEQUENCE pmnVITAL_seq');
END;
/

create sequence  pmnVITAL_seq
/

create or replace trigger pmnVITAL_trg
before insert on pmnVITAL
for each row
begin
  select pmnVITAL_seq.nextval into :new.VITALIID from dual;
end;
/


BEGIN
PMN_DROPSQL('DROP TABLE pmnPROCEDURE');
END;
/

CREATE TABLE pmnPROCEDURE(
	PROCEDURESID NUMBER(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID varchar(50) NOT NULL,
	ENC_TYPE varchar(2) NULL,
	ADMIT_DATE date NULL,
	PROVIDERID varchar(50) NULL,
	PX_DATE date NULL,
	PX varchar(11) NOT NULL,
	PX_TYPE varchar(2) NOT NULL,
	PX_SOURCE varchar(2) NULL,
	RAW_PX varchar(50) NULL,
	RAW_PX_TYPE varchar(50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  pmnPROCEDURE_seq');
END;
/

create sequence  pmnPROCEDURE_seq
/

create or replace trigger pmnPROCEDURE_trg
before insert on pmnPROCEDURE
for each row
begin
  select pmnPROCEDURE_seq.nextval into :new.PROCEDURESID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE pmndiagnosis');
END;
/

CREATE TABLE pmndiagnosis(
	DIAGNOSISID NUMBER(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID varchar(50) NOT NULL,
	ENC_TYPE varchar(2) NULL,
	ADMIT_DATE date NULL,
	PROVIDERID varchar(50) NULL,
	DX varchar(18) NOT NULL,
	DX_TYPE varchar(2) NOT NULL,
	DX_SOURCE varchar(2) NOT NULL,
	PDX varchar(2) NULL,
	RAW_DX varchar(50) NULL,
	RAW_DX_TYPE varchar(50) NULL,
	RAW_DX_SOURCE varchar(50) NULL,
	RAW_ORIGDX varchar(50) NULL,
	RAW_PDX varchar(50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  pmndiagnosis_seq');
END;
/
create sequence  pmndiagnosis_seq
/

create or replace trigger pmndiagnosis_trg
before insert on pmndiagnosis
for each row
begin
  select pmndiagnosis_seq.nextval into :new.DIAGNOSISID from dual;
end;
/




BEGIN
PMN_DROPSQL('DROP TABLE pmnlabresults_cm');
END;
/

CREATE TABLE pmnlabresults_cm(
	LAB_RESULT_CM_ID NUMBER(19)  primary key,
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
	RESULT_NUM number (18,5) NULL,
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
PMN_DROPSQL('DROP SEQUENCE pmnlabresults_cm_seq');
END;
/

create sequence  pmnlabresults_cm_seq
/

create or replace trigger pmnlabresults_cm_trg
before insert on pmnlabresults_cm
for each row
begin
  select pmnlabresults_cm_seq.nextval into :new.LAB_RESULT_CM_ID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE PMN_LabNormal');
END;
/
CREATE TABLE PMN_LabNormal  ( 
	LAB_NAME          	varchar(150) NULL,
	NORM_RANGE_LOW    	varchar(10) NULL,
	NORM_MODIFIER_LOW 	varchar(2) NULL,
	NORM_RANGE_HIGH   	varchar(10) NULL,
	NORM_MODIFIER_HIGH	varchar(2) NULL 
	)
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:LDL', '0', 'GE', '165', 'LE')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:A1C', '', 'NI', '', 'NI')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:CK', '50', 'GE', '236', 'LE')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:CK_MB', '', 'NI', '', 'NI')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:CK_MBI', '', 'NI', '', 'NI')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:CREATININE', '0', 'GE', '1.6', 'LE')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:CREATININE', '0', 'GE', '1.6', 'LE')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:HGB', '12', 'GE', '17.5', 'LE')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:INR', '0.8', 'GE', '1.3', 'LE')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:TROP_I', '0', 'GE', '0.49', 'LE')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:TROP_T_QL', '', 'NI', '', 'NI')
/

INSERT INTO PMN_LabNormal(LAB_NAME, NORM_RANGE_LOW, NORM_MODIFIER_LOW, NORM_RANGE_HIGH, NORM_MODIFIER_HIGH)
  VALUES('LAB_NAME:TROP_T_QN', '0', 'GE', '0.09', 'LE')
/

BEGIN
PMN_DROPSQL('DROP TABLE pmndeath');
END;
/
CREATE TABLE pmndeath(
	PATID varchar(50) NOT NULL,
	DEATH_DATE date NOT NULL,
	DEATH_DATE_IMPUTE varchar(2) NULL,
	DEATH_SOURCE varchar(2) NOT NULL,
	DEATH_MATCH_CONFIDENCE varchar(2) NULL
)
/

BEGIN
PMN_DROPSQL('DROP TABLE pmndeath_cause');
END;
/
CREATE TABLE pmndeath_cause(
	PATID varchar(50) NOT NULL,
	DEATH_CAUSE varchar(8) NOT NULL,
	DEATH_CAUSE_CODE varchar(2) NOT NULL,
	DEATH_CAUSE_TYPE varchar(2) NOT NULL,
	DEATH_CAUSE_SOURCE varchar(2) NOT NULL,
	DEATH_CAUSE_CONFIDENCE varchar(2) NULL
)
/

BEGIN
PMN_DROPSQL('DROP TABLE pmndispensing');
END;
/
CREATE TABLE pmndispensing(
	DISPENSINGID NUMBER(19)  primary key,
	PATID varchar(50) NOT NULL,
	PRESCRIBINGID NUMBER(19)  NULL, -- jgk fix 9/24
	DISPENSE_DATE date NOT NULL,
	NDC varchar (11) NOT NULL,
	DISPENSE_SUP int, 
	DISPENSE_AMT int, 
	RAW_NDC varchar (50)
)
/


BEGIN
PMN_DROPSQL('DROP sequence  pmndispensing_seq');
END;
/
create sequence  pmndispensing_seq
/

create or replace trigger pmndispensing_trg
before insert on pmndispensing
for each row
begin
  select pmndispensing_seq.nextval into :new.DISPENSINGID from dual;
end;
/










BEGIN
PMN_DROPSQL('DROP TABLE pmnprescribing');
END;
/
CREATE TABLE pmnprescribing(
	PRESCRIBINGID NUMBER(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID  varchar(50) NULL,
	RX_PROVIDERID varchar(50) NULL, -- NOTE: The spec has a _ before the ID, but this is inconsistent.
	RX_ORDER_DATE date NULL,
	RX_ORDER_TIME varchar (5) NULL,
	RX_START_DATE date NULL,
	RX_END_DATE date NULL,
	RX_QUANTITY int NULL,
	RX_REFILLS int NULL,
	RX_DAYS_SUPPLY int NULL,
	RX_FREQUENCY int NULL,
	RX_BASIS varchar (2) NULL,
	RXNORM_CUI int NULL,
	RAW_RX_MED_NAME varchar (50) NULL,
	RAW_RX_FREQUENCY varchar (50) NULL,
	RAW_RXNORM_CUI varchar (50) NULL
)
/


BEGIN
PMN_DROPSQL('DROP sequence  pmnprescribing_seq');
END;
/
create sequence  pmnprescribing_seq
/

create or replace trigger pmnprescribing_trg
before insert on pmnprescribing
for each row
begin
  select pmnprescribing_seq.nextval into :new.PRESCRIBINGID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE pmnpcornet_trial');
END;
/
CREATE TABLE pmnpcornet_trial(
	PATID varchar(50) NOT NULL,
	TRIAL_ID varchar(20) NOT NULL,
	PARTICIPANTID varchar(50) NOT NULL,
	TRIAL_SITEID varchar(50) NULL,
	TRIAL_ENROLL_DATE date NULL,
	TRIAL_END_DATE date NULL,
	TRIAL_WITHDRAW_DATE date NULL,
	TRIAL_INVITE_CODE varchar(20) NULL
)
/

BEGIN
PMN_DROPSQL('DROP TABLE pmncondition');
END;
/
CREATE TABLE pmncondition(
	CONDITIONID NUMBER(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID  varchar(50) NULL,
	REPORT_DATE  date NULL,
	RESOLVE_DATE  date NULL,
	ONSET_DATE  date NULL,
	CONDITION_STATUS varchar(2) NULL,
	CONDITION varchar(18) NOT NULL,
	CONDITION_TYPE varchar(2) NOT NULL,
	CONDITION_SOURCE varchar(2) NOT NULL,
	RAW_CONDITION_STATUS varchar(2) NULL,
	RAW_CONDITION varchar(18) NULL,
	RAW_CONDITION_TYPE varchar(2) NULL,
	RAW_CONDITION_SOURCE varchar(2) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  pmncondition_seq');
END;
/
create sequence  pmncondition_seq
/

create or replace trigger pmncondition_trg
before insert on pmncondition
for each row
begin
  select pmncondition_seq.nextval into :new.CONDITIONID from dual;
end;
/



BEGIN
PMN_DROPSQL('DROP TABLE pmnpro_cm');
END;
/
CREATE TABLE pmnpro_cm(
	PRO_CM_ID NUMBER(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID  varchar(50) NULL,
	PRO_ITEM varchar (7) NOT NULL,
	PRO_LOINC varchar (10) NULL,
	PRO_DATE date NOT NULL,
	PRO_TIME varchar (5) NULL,
	PRO_RESPONSE int NOT NULL,
	PRO_METHOD varchar (2) NULL,
	PRO_MODE varchar (2) NULL,
	PRO_CAT varchar (2) NULL,
	RAW_PRO_CODE varchar (50) NULL,
	RAW_PRO_RESPONSE varchar (50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  pmnpro_cm_seq');
END;
/
create sequence  pmnpro_cm_seq
/

create or replace trigger pmnpro_cm_trg
before insert on pmnpro_cm
for each row
begin
  select pmnpro_cm_seq.nextval into :new.PRO_CM_ID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE pmnharvest');
END;
/
CREATE TABLE pmnharvest(
	NETWORKID varchar(10) NOT NULL,
	NETWORK_NAME varchar(20) NULL,
	DATAMARTID varchar(10) NOT NULL,
	DATAMART_NAME varchar(20) NULL,
	DATAMART_PLATFORM varchar(2) NULL,
	CDM_VERSION numeric(8, 2) NULL,
	DATAMART_CLAIMS varchar(2) NULL,
	DATAMART_EHR varchar(2) NULL,
	BIRTH_DATE_MGMT varchar(2) NULL,
	ENR_START_DATE_MGMT varchar(2) NULL,
	ENR_END_DATE_MGMT varchar(2) NULL,
	ADMIT_DATE_MGMT varchar(2) NULL,
	DISCHARGE_DATE_MGMT varchar(2) NULL,
	PX_DATE_MGMT varchar(2) NULL,
	RX_ORDER_DATE_MGMT varchar(2) NULL,
	RX_START_DATE_MGMT varchar(2) NULL,
	RX_END_DATE_MGMT varchar(2) NULL,
	DISPENSE_DATE_MGMT varchar(2) NULL,
	LAB_ORDER_DATE_MGMT varchar(2) NULL,
	SPECIMENT_DATE_MGMT varchar(2) NULL,
	RESULT_DATE_MGMT varchar(2) NULL,
	MEASURE_DATE_MGMT varchar(2) NULL,
	ONSET_DATE_MGMT varchar(2) NULL,
	REPORT_DATE_MGMT varchar(2) NULL,
	RESOLVE_DATE_MGMT varchar(2) NULL,
	PRO_DATE_MGMT varchar(2) NULL,
	REFRESH_DEMOGRAPHIC_DATE date NULL,
	REFRESH_ENROLLMENT_DATE date NULL,
	REFRESH_ENCOUNTER_DATE date NULL,
	REFRESH_DIAGNOSIS_DATE date NULL,
	REFRESH_PROCEDURES_DATE date NULL,
	REFRESH_VITAL_DATE date NULL,
	REFRESH_DISPENSING_DATE date NULL,
	REFRESH_LAB_RESULT_CM_DATE date NULL,
	REFRESH_CONDITION_DATE date NULL,
	REFRESH_PRO_CM_DATE date NULL,
	REFRESH_PRESCRIBING_DATE date NULL,
	REFRESH_PCORNET_TRIAL_DATE date NULL,
	REFRESH_DEATH_DATE date NULL,
	REFRESH_DEATH_CAUSE_DATE date NULL
)
/



BEGIN
PMN_DROPSQL('DROP TABLE pmnENCOUNTER');
END;
/
CREATE TABLE pmnENCOUNTER(
	PATID varchar(50) NOT NULL,
	ENCOUNTERID varchar(50) NOT NULL,
	ADMIT_DATE date NULL,
	ADMIT_TIME varchar(5) NULL,
	DISCHARGE_DATE date NULL,
	DISCHARGE_TIME varchar(5) NULL,
	PROVIDERID varchar(50) NULL,
	FACILITY_LOCATION varchar(3) NULL,
	ENC_TYPE varchar(2) NOT NULL,
	FACILITYID varchar(50) NULL,
	DISCHARGE_DISPOSITION varchar(2) NULL,
	DISCHARGE_STATUS varchar(2) NULL,
	DRG varchar(3) NULL,
	DRG_TYPE varchar(2) NULL,
	ADMITTING_SOURCE varchar(2) NULL,
	RAW_SITEID varchar (50) NULL,
	RAW_ENC_TYPE varchar(50) NULL,
	RAW_DISCHARGE_DISPOSITION varchar(50) NULL,
	RAW_DISCHARGE_STATUS varchar(50) NULL,
	RAW_DRG_TYPE varchar(50) NULL,
	RAW_ADMITTING_SOURCE varchar(50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP TABLE pmndemographic');
END;
/
CREATE TABLE pmndemographic(
	PATID varchar(50) NOT NULL,
	BIRTH_DATE date NULL,
	BIRTH_TIME varchar(5) NULL,
	SEX varchar(2) NULL,
	HISPANIC varchar(2) NULL,
	BIOBANK_FLAG varchar(1) DEFAULT 'N',
	RACE varchar(2) NULL,
	RAW_SEX varchar(50) NULL,
	RAW_HISPANIC varchar(50) NULL,
	RAW_RACE varchar(50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP TABLE i2pReport');
END;
/

create table i2pReport (runid number, rundate date, concept varchar(20), sourceval number, destval number, diff number)
/

BEGIN
insert into i2preport (runid) values (0);
pcornet_popcodelist;
END;
/







create or replace procedure PCORNetDemographic as 

sqltext varchar2(4000); 
cursor getsql is 
--1 --  S,R,NH
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''1'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	''''||sex.pcori_basecode||''','||
	'''NI'','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(p.sex_cd) in ('||lower(sex.c_dimcode)||') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'   and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo race, pcornet_demo sex
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union -- A - S,R,H
select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''A'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	''''||sex.pcori_basecode||''','||
	''''||hisp.pcori_basecode||''','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(p.sex_cd) in ('||lower(sex.c_dimcode)||') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'	and	lower(nvl(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''RACE'') '||
	'   and lower(nvl(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo race, pcornet_demo hisp, pcornet_demo sex
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC\Y%'
	and hisp.c_visualattributes like 'L%'
	and sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --2 S, nR, nH
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''2'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	''''||sex.pcori_basecode||''','||
	'''NI'','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) in ('||lower(sex.c_dimcode)||') '||
	'	and	lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '||
	'   and lower(nvl(p.race_cd,''ni'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo sex
	where sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --3 -- nS,R, NH
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''3'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	'''NI'','||
	'''NI'','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'   and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'')'
	from pcornet_demo race
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
union --B -- nS,R, H
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''B'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	'''NI'','||
	''''||hisp.pcori_basecode||''','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'	and	lower(nvl(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''RACE'') '||
	'   and lower(nvl(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'')'
	from pcornet_demo race,pcornet_demo hisp
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC\Y%'
	and hisp.c_visualattributes like 'L%'
union --4 -- S, NR, H
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''4'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	''''||sex.pcori_basecode||''','||
	'''Y'','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''NI'')) in ('||lower(sex.c_dimcode)||') '||
	'	and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '||
	'	and lower(nvl(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo sex
	where sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --5 -- NS, NR, H
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''5'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	'''NI'','||
	'''Y'','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '||
	'	and lower(nvl(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'')'
	from dual
union --6 -- NS, NR, nH
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''6'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
	'''NI'','||
	'''NI'','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '||
	'   and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') ' 
	from dual;

begin    
pcornet_popcodelist;

OPEN getsql;
LOOP
FETCH getsql INTO sqltext;
	EXIT WHEN getsql%NOTFOUND;  
--	insert into st values (sqltext); 
	execute immediate sqltext; 
	COMMIT;
END LOOP;
CLOSE getsql;
end PCORNetDemographic; 
/



create or replace procedure PCORNetEncounter as

sqltext varchar2(4000);
begin

insert into pmnencounter(PATID,ENCOUNTERID,admit_date ,ADMIT_TIME , 
		DISCHARGE_DATE ,DISCHARGE_TIME ,PROVIDERID ,FACILITY_LOCATION  
		,ENC_TYPE ,FACILITYID ,DISCHARGE_DISPOSITION , 
		DISCHARGE_STATUS ,DRG ,DRG_TYPE ,ADMITTING_SOURCE) 
select distinct v.patient_num, v.encounter_num,  
	start_Date, 
	to_char(start_Date,'HH:MI'), 
	end_Date, 
	to_char(end_Date,'HH:MI'), 
	providerid,location_zip, 
(case when pcori_enctype is not null then pcori_enctype else 'UN' end) enc_type, facility_id,  CASE WHEN pcori_enctype='AV' THEN 'NI' ELSE  discharge_disposition END , CASE WHEN pcori_enctype='AV' THEN 'NI' ELSE discharge_status END  , drg.drg, drg_type, CASE WHEN _source IS NULL THEN 'NI' ELSE admitting_source END  
from i2b2visit v inner join pmndemographic d on v.patient_num=d.patid
left outer join 
   (select * from
   (select patient_num,encounter_num,drg_type, drg,row_number() over (partition by  patient_num, encounter_num order by drg_type desc) AS rn from 
   (select patient_num,encounter_num,drg_type,max(drg) drg  from
    (select distinct f.patient_num,encounter_num,SUBSTR(c_fullname,22,2) drg_type,SUBSTR(pcori_basecode,INSTR(pcori_basecode, ':')+1,3) drg from i2b2fact f 
     inner join pmndemographic d on f.patient_num=d.patid
     inner join pcornet_enc enc on enc.c_basecode  = f.concept_cd   
      and enc.c_fullname like '\PCORI\ENCOUNTER\DRG\%') drg1 group by patient_num,encounter_num,drg_type) drg) drg
     where rn=1) drg -- This section is bugfixed to only include 1 drg if multiple DRG types exist in a single encounter...
  on drg.patient_num=v.patient_num and drg.encounter_num=v.encounter_num
left outer join 
-- Encounter type. Note that this requires a full table scan on the ontology table, so it is not particularly efficient.
(select patient_num, encounter_num, inout_cd,SUBSTR(pcori_basecode,INSTR(pcori_basecode, ':')+1,2) pcori_enctype from i2b2visit v
 inner join pcornet_enc e on c_dimcode like '%'''||inout_cd||'''%' and e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%') enctype
  on enctype.patient_num=v.patient_num and enctype.encounter_num=v.encounter_num;

end PCORNetEncounter;
/




create or replace procedure PCORNetDiagnosis as
sqltext varchar2(4000);
begin

PMN_DROPSQL('DROP TABLE sourcefact');

sqltext := 'create table sourcefact as '||
	'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode dxsource, dxsource.c_fullname '||
	'from i2b2fact factline '||
    'inner join pmnENCOUNTER enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
    'inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode '||
	'where dxsource.c_fullname like ''\PCORI_MOD\CONDITION_OR_DX\%''';
PMN_EXECUATESQL(sqltext);


PMN_DROPSQL('DROP TABLE pdxfact');

sqltext := 'create table pdxfact as '||
	'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode pdxsource,dxsource.c_fullname  '||
	'from i2b2fact factline '||
    'inner join pmnENCOUNTER enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
    'inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode '||
	'and dxsource.c_fullname like ''\PCORI_MOD\PDX\%''';
PMN_EXECUATESQL(sqltext);


sqltext := 'insert into pmndiagnosis (patid,			encounterid,	enc_type, admit_date, providerid, dx, dx_type, dx_source, pdx) '||
'select distinct factline.patient_num, factline.encounter_num encounterid,	enc_type, factline.start_date, factline.provider_id, diag.pcori_basecode,  '||
'SUBSTR(diag.c_fullname,18,2) dxtype,   '||
'	CASE WHEN enc_type=''AV'' THEN ''FI'' ELSE nvl(SUBSTR(dxsource,INSTR(dxsource,'':'')+1,2) ,''NI'')END, '||
'	nvl(SUBSTR(pdxsource,INSTR(pdxsource, '':'')+1,2),''NI'') '|| -- jgk bugfix 9/28/15 
'from i2b2fact factline '||
'inner join pmnENCOUNTER enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
' left outer join sourcefact '||
'on	factline.patient_num=sourcefact.patient_num '||
'and factline.encounter_num=sourcefact.encounter_num '||
'and factline.provider_id=sourcefact.provider_id '||
'and factline.concept_cd=sourcefact.concept_Cd '||
'and factline.start_date=sourcefact.start_Date '||
'left outer join pdxfact '||
'on	factline.patient_num=pdxfact.patient_num '||
'and factline.encounter_num=pdxfact.encounter_num '||
'and factline.provider_id=pdxfact.provider_id '||
'and factline.concept_cd=pdxfact.concept_cd '||
'and factline.start_date=pdxfact.start_Date '||
'inner join pcornet_diag diag on diag.c_basecode  = factline.concept_cd '||
'where diag.c_fullname like ''\PCORI\DIAGNOSIS\%''  '||
'and (sourcefact.c_fullname like ''\PCORI_MOD\CONDITION_OR_DX\DX_SOURCE\%'' or sourcefact.c_fullname is null) ';

PMN_EXECUATESQL(sqltext);


end PCORNetDiagnosis;
/





create or replace procedure PCORNetCondition as
sqltext varchar2(4000);
begin


PMN_DROPSQL('DROP TABLE sourcefact2');

sqltext := 'create table sourcefact2 as '||
	'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode dxsource, dxsource.c_fullname '||
	'from i2b2fact factline '||
    'inner join pmnENCOUNTER enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
    'inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode '||
	'where dxsource.c_fullname like ''\PCORI_MOD\CONDITION_OR_DX\%''';
PMN_EXECUATESQL(sqltext);

sqltext := 'insert into pmncondition (patid, encounterid, report_date, resolve_date, condition, condition_type, condition_status, condition_source) '||
'select distinct factline.patient_num, min(factline.encounter_num) encounterid, min(factline.start_date) report_date, NVL(max(factline.end_date),null) resolve_date, diag.pcori_basecode,  '||
'SUBSTR(diag.c_fullname,18,2) condition_type,   '||
'	NVL2(max(factline.end_date) , ''RS'', ''NI'') condition_status,  '|| -- Imputed so might not be entirely accurate
'	NVL(SUBSTR(max(dxsource),INSTR(max(dxsource), '':'')+1,2),''NI'') condition_source '||
'from i2b2fact factline '||
'inner join pmnENCOUNTER enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num '||
'inner join pcornet_diag diag on diag.c_basecode  = factline.concept_cd    '||
' left outer join sourcefact2 sf '||
'on	factline.patient_num=sf.patient_num '||
'and factline.encounter_num=sf.encounter_num '||
'and factline.provider_id=sf.provider_id '||
'and factline.concept_cd=sf.concept_Cd '||
'and factline.start_date=sf.start_Date   '||
'where diag.c_fullname like ''\PCORI\DIAGNOSIS\%'' '||
'and sf.c_fullname like ''\PCORI_MOD\CONDITION_OR_DX\CONDITION_SOURCE\%'' '||
'group by factline.patient_num, diag.pcori_basecode, diag.c_fullname ';

PMN_EXECUATESQL(sqltext);

end PCORNetCondition;
/







create or replace procedure PCORNetProcedure as
begin
insert into pmnprocedure( 
				patid,			encounterid,	enc_type, admit_date, providerid, px, px_type) 
select  distinct fact.patient_num, enc.encounterid,	enc.enc_type, fact.start_date, 
		fact.provider_id, SUBSTR(pr.pcori_basecode,INSTR(pr.pcori_basecode, ':')+1,11) px, SUBSTR(pr.c_fullname,18,2) pxtype 
from i2b2fact fact
 inner join pmnENCOUNTER enc on enc.patid = fact.patient_num and enc.encounterid = fact.encounter_Num
 inner join	pcornet_proc pr on pr.c_basecode  = fact.concept_cd   
where pr.c_fullname like '\PCORI\PROCEDURE\%';

end PCORNetProcedure;
/







create or replace procedure PCORNetVital as
begin
-- jgk: I took out admit_date - it doesn't appear in the scheme. Now in SQLServer format - date, substring, name on inner select, no nested with. Added modifiers and now use only pathnames, not codes.
insert into pmnVITAL(patid, encounterid, measure_date, measure_time,vital_source,ht, wt, diastolic, systolic, original_bmi, bp_position,smoking,tobacco,tobacco_type)
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
  from pmndemographic pd
  left join (
    select 
      obs.patient_num patid, obs.encounter_num encounterid, 
	to_char(obs.start_Date,'YYYY-MM-DD') measure_date, 
	to_char(obs.start_Date,'HH:MI') measure_time, 
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
  join pmnencounter enc on enc.encounterid = vit.encounterid
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

end PCORNetVital;
/






create or replace procedure PCORNetEnroll as
begin

INSERT INTO pmnENROLLMENT(PATID, ENR_START_DATE, ENR_END_DATE, CHART, BASIS) 
    select x.patient_num patid, case when l.patient_num is not null then l.period_start else enr_start end enr_start_date
    , case when l.patient_num is not null then l.period_end when enr_end_end>enr_end then enr_end_end else enr_end end enr_end_date 
    , 'Y' chart, case when l.patient_num is not null then 'A' else 'E' end basis from 
    (select patient_num, min(start_date) enr_start,max(start_date) enr_end,max(end_date) enr_end_end from i2b2visit where patient_num in (select patid from pmndemographic) group by patient_num) x
    left outer join i2b2loyalty_patients l on l.patient_num=x.patient_num;

end PCORNetEnroll;
/


create or replace procedure PCORNetLabResultCM as
sqltext varchar2(4000);
begin
PMN_DROPSQL('DROP TABLE priority');

sqltext := 'create table priority as '||
'(select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, lsource.pcori_basecode  PRIORITY  '||
'from i2b2fact '||
'inner join pmnENCOUNTER enc on enc.patid = i2b2fact.patient_num and enc.encounterid = i2b2fact.encounter_Num '||
'inner join pcornet_lab lsource on i2b2fact.modifier_cd =lsource.c_basecode '||
'where c_fullname LIKE ''\PCORI_MOD\PRIORITY\%'') ';

PMN_EXECUATESQL(sqltext);


PMN_DROPSQL('DROP TABLE location');
sqltext := 'create table location as '||
'(select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, lsource.pcori_basecode  RESULT_LOC '||
'from i2b2fact '||
'inner join pmnENCOUNTER enc on enc.patid = i2b2fact.patient_num and enc.encounterid = i2b2fact.encounter_Num '||
'inner join pcornet_lab lsource on i2b2fact.modifier_cd =lsource.c_basecode '||
'where c_fullname LIKE ''\PCORI_MOD\RESULT_LOC\%'') ';

PMN_EXECUATESQL(sqltext);


INSERT INTO pmnlabresults_cm
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
CASE WHEN ont_parent.C_BASECODE LIKE 'LAB_NAME%' then SUBSTR (ont_parent.c_basecode,10, 10) ELSE 'NI' END LAB_NAME,
CASE WHEN lab.pcori_specimen_source like '%or SR_PLS' THEN 'SR_PLS' WHEN lab.pcori_specimen_source is null then 'NI' ELSE lab.pcori_specimen_source END specimen_source, -- (Better way would be to fix the column in the ontology but this will work)
NVL(lab.pcori_basecode, 'NI') LAB_LOINC,
NVL(p.PRIORITY,'NI') PRIORITY,
NVL(l.RESULT_LOC,'NI') RESULT_LOC,
NVL(lab.pcori_basecode, 'NI') LAB_PX,
'LC'  LAB_PX_TYPE,
m.start_date LAB_ORDER_DATE, 
m.start_date SPECIMEN_DATE,
to_char(m.start_date,'HH:MI')  SPECIMEN_TIME,
m.end_date RESULT_DATE,
to_char(m.end_date,'HH:MI') RESULT_TIME,
CASE WHEN m.ValType_Cd='T' THEN NVL(nullif(m.TVal_Char,''),'NI') ELSE 'NI' END RESULT_QUAL, -- TODO: Should be a standardized value
CASE WHEN m.ValType_Cd='N' THEN m.NVAL_NUM ELSE null END RESULT_NUM,
CASE WHEN m.ValType_Cd='N' THEN (CASE NVL(nullif(m.TVal_Char,''),'NI') WHEN 'E' THEN 'EQ' WHEN 'NE' THEN 'OT' WHEN 'L' THEN 'LT' WHEN 'LE' THEN 'LE' WHEN 'G' THEN 'GT' WHEN 'GE' THEN 'GE' ELSE 'NI' END)  ELSE 'TX' END RESULT_MODIFIER,
NVL(m.Units_CD,'NI') RESULT_UNIT, -- TODO: Should be standardized units
nullif(norm.NORM_RANGE_LOW,'') NORM_RANGE_LOW
,norm.NORM_MODIFIER_LOW,
nullif(norm.NORM_RANGE_HIGH,'') NORM_RANGE_HIGH
,norm.NORM_MODIFIER_HIGH,
CASE NVL(nullif(m.VALUEFLAG_CD,''),'NI') WHEN 'H' THEN 'AH' WHEN 'L' THEN 'AL' WHEN 'A' THEN 'AB' ELSE 'NI' END ABN_IND,
NULL RAW_LAB_NAME,
NULL RAW_LAB_CODE,
NULL RAW_PANEL,
CASE WHEN m.ValType_Cd='T' THEN m.TVal_Char ELSE to_char(m.NVal_Num) END RAW_RESULT,
NULL RAW_UNIT,
NULL RAW_ORDER_DEPT,
NULL RAW_FACILITY_CODE

FROM i2b2fact M
inner join pmnENCOUNTER enc on enc.patid = m.patient_num and enc.encounterid = m.encounter_Num -- Constraint to selected encounters

inner join pcornet_lab lab on lab.c_basecode  = M.concept_cd and lab.c_fullname like '\PCORI\LAB_RESULT_CM\%'
inner JOIN pcornet_lab ont_parent on lab.c_path=ont_parent.c_fullname
inner join pmn_labnormal norm on ont_parent.c_basecode=norm.LAB_NAME

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
 
WHERE m.ValType_Cd in ('N','T')
and ont_parent.C_BASECODE LIKE 'LAB_NAME%' -- Exclude non-pcori labs
and m.MODIFIER_CD='@';

END PCORNetLabResultCM;
/




create or replace procedure PCORNetHarvest as
begin

INSERT INTO pmnharvest(NETWORKID, NETWORK_NAME, DATAMARTID, DATAMART_NAME, DATAMART_PLATFORM, CDM_VERSION, DATAMART_CLAIMS, DATAMART_EHR, BIRTH_DATE_MGMT, ENR_START_DATE_MGMT, ENR_END_DATE_MGMT, ADMIT_DATE_MGMT, DISCHARGE_DATE_MGMT, PX_DATE_MGMT, RX_ORDER_DATE_MGMT, RX_START_DATE_MGMT, RX_END_DATE_MGMT, DISPENSE_DATE_MGMT, LAB_ORDER_DATE_MGMT, SPECIMENT_DATE_MGMT, RESULT_DATE_MGMT, MEASURE_DATE_MGMT, ONSET_DATE_MGMT, REPORT_DATE_MGMT, RESOLVE_DATE_MGMT, PRO_DATE_MGMT, REFRESH_DEMOGRAPHIC_DATE, REFRESH_ENROLLMENT_DATE, REFRESH_ENCOUNTER_DATE, REFRESH_DIAGNOSIS_DATE, REFRESH_PROCEDURES_DATE, REFRESH_VITAL_DATE, REFRESH_DISPENSING_DATE, REFRESH_LAB_RESULT_CM_DATE, REFRESH_CONDITION_DATE, REFRESH_PRO_CM_DATE, REFRESH_PRESCRIBING_DATE, REFRESH_PCORNET_TRIAL_DATE, REFRESH_DEATH_DATE, REFRESH_DEATH_CAUSE_DATE) 
	VALUES('SCILHS', 'SCILHS', getDataMartID(), getDataMartName(), getDataMartPlatform(), 3, 01, 02, 1,1,2,1,2,1,2,1,2,1,1,2,2,1,1,1,2,1,current_date,current_date,current_date,current_date,current_date,current_date,current_date,current_date,current_date,null,current_date,null,null,null);

end PCORNetHarvest;
/






create or replace procedure PCORNetPrescribing as
sqltext varchar2(4000);
begin

PMN_DROPSQL('DROP TABLE basis');
sqltext := 'create table basis as '||
'(select pcori_basecode,c_fullname,encounter_num,concept_cd from i2b2fact basis '||
'        inner join pmnENCOUNTER enc on enc.patid = basis.patient_num and enc.encounterid = basis.encounter_Num '||
'     join pcornet_med basiscode  '||
'        on basis.modifier_cd = basiscode.c_basecode '||
'        and basiscode.c_fullname like ''\PCORI_MOD\RX_BASIS\%'') ';
PMN_EXECUATESQL(sqltext);

PMN_DROPSQL('DROP TABLE freq');
sqltext := 'create table freq as '||
'(select pcori_basecode,encounter_num,concept_cd from i2b2fact freq '||
'        inner join pmnENCOUNTER enc on enc.patid = freq.patient_num and enc.encounterid = freq.encounter_Num '||
'     join pcornet_med freqcode  '||
'        on freq.modifier_cd = freqcode.c_basecode '||
'        and freqcode.c_fullname like ''\PCORI_MOD\RX_FREQUENCY\%'') ';
PMN_EXECUATESQL(sqltext);

PMN_DROPSQL('DROP TABLE quantity');
sqltext := 'create table quantity as '||
'(select nval_num,encounter_num,concept_cd from i2b2fact quantity '||
'        inner join pmnENCOUNTER enc on enc.patid = quantity.patient_num and enc.encounterid = quantity.encounter_Num '||
'     join pcornet_med quantitycode  '||
'        on quantity.modifier_cd = quantitycode.c_basecode '||
'        and quantitycode.c_fullname like ''\PCORI_MOD\RX_QUANTITY\'') ';

PMN_EXECUATESQL(sqltext);
        
PMN_DROPSQL('DROP TABLE refills');
sqltext := 'create table refills as   '||
'(select nval_num,encounter_num,concept_cd from i2b2fact refills '||
'        inner join pmnENCOUNTER enc on enc.patid = refills.patient_num and enc.encounterid = refills.encounter_Num '||
'     join pcornet_med refillscode  '||
'        on refills.modifier_cd = refillscode.c_basecode '||
'        and refillscode.c_fullname like ''\PCORI_MOD\RX_REFILLS\'') ';
PMN_EXECUATESQL(sqltext);

PMN_DROPSQL('DROP TABLE supply');  
sqltext := 'create table supply as  '||
'(select nval_num,encounter_num,concept_cd from i2b2fact supply '||
'        inner join pmnENCOUNTER enc on enc.patid = supply.patient_num and enc.encounterid = supply.encounter_Num '||
'     join pcornet_med supplycode  '||
'        on supply.modifier_cd = supplycode.c_basecode '||
'        and supplycode.c_fullname like ''\PCORI_MOD\RX_DAYS_SUPPLY\'')  ';
PMN_EXECUATESQL(sqltext);


-- insert data with outer joins to ensure all records are included even if some data elements are missing
insert into pmnprescribing (
	PATID
    ,encounterid
    ,RX_PROVIDERID
	,RX_ORDER_DATE -- using start_date from i2b2
	,RX_ORDER_TIME  -- using time start_date from i2b2
	,RX_START_DATE
	,RX_END_DATE 
    ,RXNORM_CUI --using pcornet_med pcori_cui - new column!
    ,RX_QUANTITY ---- modifier nval_num
    ,RX_REFILLS  -- modifier nval_num
    ,RX_DAYS_SUPPLY -- modifier nval_num
    ,RX_FREQUENCY --modifier with basecode lookup
    ,RX_BASIS --modifier with basecode lookup
--    ,RAW_RX_MED_NAME, --not filling these right now
--    ,RAW_RX_FREQUENCY,
--    ,RAW_RXNORM_CUI
)
select distinct  m.patient_num, m.Encounter_Num,m.provider_id,  m.start_date order_date,  to_char(m.start_date,'HH:MI'), m.start_date start_date, m.end_date, mo.pcori_cui
    ,quantity.nval_num quantity, refills.nval_num refills, supply.nval_num supply, freq.pcori_basecode frequency, basis.pcori_basecode basis
 from i2b2fact m inner join pcornet_med mo on m.concept_cd = mo.c_basecode 
inner join pmnENCOUNTER enc on enc.encounterid = m.encounter_Num
-- TODO: This join adds several minutes to the load - must be debugged

    left join basis
    on m.encounter_num = basis.encounter_num
    and m.concept_cd = basis.concept_Cd

    left join  freq
    on m.encounter_num = freq.encounter_num
    and m.concept_cd = freq.concept_Cd

    left join quantity 
    on m.encounter_num = quantity.encounter_num
    and m.concept_cd = quantity.concept_Cd

    left join refills
    on m.encounter_num = refills.encounter_num
    and m.concept_cd = refills.concept_Cd

    left join supply
    on m.encounter_num = supply.encounter_num
    and m.concept_cd = supply.concept_Cd

where (basis.c_fullname is null or basis.c_fullname= '\PCORI_MOD\RX_BASIS\PR\%'); -- jgk 11/2 bugfix: filter for PR, not DI

end PCORNetPrescribing;
/



create or replace procedure PCORNetDispensing as
sqltext varchar2(4000);
begin
PMN_DROPSQL('DROP TABLE supply');
sqltext := 'create table supply as '||
'(select nval_num,encounter_num,concept_cd from i2b2fact supply '||
'        inner join pmnENCOUNTER enc on enc.patid = supply.patient_num and enc.encounterid = supply.encounter_Num '||
'      join pcornet_med supplycode  '||
'        on supply.modifier_cd = supplycode.c_basecode '||
'        and supplycode.c_fullname like ''\PCORI_MOD\RX_DAYS_SUPPLY\'' ) ';
PMN_EXECUATESQL(sqltext);


PMN_DROPSQL('DROP TABLE amount');
sqltext := 'create table amount as '||
'(select nval_num,encounter_num,concept_cd from i2b2fact amount '||
'     join pcornet_med amountcode '||
'        on amount.modifier_cd = amountcode.c_basecode '||
'        and amountcode.c_fullname like ''\PCORI_MOD\RX_QUANTITY\'') ';
PMN_EXECUATESQL(sqltext);
        
-- insert data with outer joins to ensure all records are included even if some data elements are missing

insert into pmndispensing (
	PATID
    ,PRESCRIBINGID
	,DISPENSE_DATE -- using start_date from i2b2
    ,NDC --using pcornet_med pcori_ndc - new column!
    ,DISPENSE_SUP ---- modifier nval_num
    ,DISPENSE_AMT  -- modifier nval_num
--    ,RAW_NDC
)
select  m.patient_num, null,m.start_date, NVL(mo.pcori_ndc,'NA')
    ,max(supply.nval_num) sup, max(amount.nval_num) amt 
from i2b2fact m inner join pcornet_med mo
on m.concept_cd = mo.c_basecode
inner join pmnENCOUNTER enc on enc.encounterid = m.encounter_Num

    -- jgk bugfix 11/2 - we weren't filtering dispensing events
    inner join (select pcori_basecode,c_fullname,encounter_num,concept_cd from i2b2fact basis
        inner join pmnENCOUNTER enc on enc.patid = basis.patient_num and enc.encounterid = basis.encounter_Num
     join pcornet_med basiscode 
        on basis.modifier_cd = basiscode.c_basecode
        and basiscode.c_fullname='\PCORI_MOD\RX_BASIS\DI\') basis
    on m.encounter_num = basis.encounter_num
    and m.concept_cd = basis.concept_Cd 

    left join  supply
    on m.encounter_num = supply.encounter_num
    and m.concept_cd = supply.concept_Cd


    left join  amount
    on m.encounter_num = amount.encounter_num
    and m.concept_cd = amount.concept_Cd

group by m.encounter_num ,m.patient_num, m.start_date,  mo.pcori_ndc;

end PCORNetDispensing;
/







create or replace PROCEDURE pcornetReport
as
i2b2pats  number;
i2b2Encounters  number;
i2b2facts number;
i2b2dxs number;
i2b2procs number;
i2b2lcs number;

pmnpats  number;
pmnencounters number;
pmndx number;
pmnprocs number;
pmnfacts number;
pmnenroll number;
pmnvital number;



pmnlabs number;
pmnprescribings number;
pmndispensings number;
pmncond number;


v_runid number;
begin
select count(*) into i2b2Pats   from i2b2patient;
select count(*) into i2b2Encounters   from i2b2visit i inner join pmndemographic d on i.patient_num=d.patid;


select count(*) into pmnPats   from pmndemographic;
select count(*) into pmnencounters   from pmnencounter e ;
select count(*) into pmndx   from pmndiagnosis;
select count(*) into pmnprocs  from pmnprocedure;

select count(*) into pmncond from pmncondition;
select count(*) into pmnenroll  from pmnenrollment;
select count(*) into pmnvital  from pmnvital;
select count(*) into pmnlabs from pmnlabresults_cm;
select count(*) into pmnprescribings from pmnprescribing;
select count(*) into pmndispensings from pmndispensing;

select max(runid) into v_runid from i2pReport;
v_runid := v_runid + 1;
insert into i2pReport values( v_runid, SYSDATE, 'Pats', i2b2pats, pmnpats, i2b2pats-pmnpats);
insert into i2pReport values( v_runid, SYSDATE, 'Enrollment', i2b2pats, pmnenroll, i2b2pats-pmnpats);

insert into i2pReport values(v_runid, SYSDATE, 'Encounters', i2b2Encounters, pmnEncounters, i2b2encounters-pmnencounters);
insert into i2pReport values(v_runid, SYSDATE, 'DX',		null,		pmndx,		null);
insert into i2pReport values(v_runid, SYSDATE, 'PX',		null,		pmnprocs,	null);
insert into i2pReport values(v_runid, SYSDATE, 'Condition',	null,		pmncond,	null);
insert into i2pReport values(v_runid, SYSDATE, 'Vital',		null,		pmnvital,	null);
insert into i2pReport values(v_runid, SYSDATE, 'Labs',		null,		pmnlabs,	null);
insert into i2pReport values(v_runid, SYSDATE, 'Prescribing',	null,		pmnprescribings,null);
insert into i2pReport values(v_runid, SYSDATE, 'Dispensing',	null,		pmndispensings,	null);

end pcornetReport;
/




create or replace procedure pcornetloader as
begin
---pcornetclear;
PCORNetHarvest;
PCORNetDemographic;
PCORNetEncounter;
PCORNetDiagnosis;
PCORNetCondition;
PCORNetProcedure;
PCORNetVital;
PCORNetEnroll;
PCORNetLabResultCM;
PCORNetPrescribing;
PCORNetDispensing;

end pcornetloader;
/






BEGIN
pcornetloader; --- you may want to run sql statements one by one in the pcornetloader procedure :)
END;
/

select concept "Data Type",sourceval "From i2b2",destval "In PopMedNet", diff "Difference" from i2preport where RUNID = (select max(RUNID) from I2PREPORT);



