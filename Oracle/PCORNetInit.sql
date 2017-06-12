-------------------------------------------------------------------------------------------
-- PCORNetInit Script
-- 
-- This script prepares target load and intermidiary transform tables, helper
-- functions and procedure, and synonyms and views of the source i2b2 source
-- tables.
-- 
-- This script should be run as the initial step of running the i2p-transform,
-- but has been seperated out from PCORNetLoader_ora.sql so that the former can
-- be run without dropping all tables.
--
-- Created by: Michael Prittie (mprittie@kumc.edu)
-- Adapted from original PCORNetLoader_ora.sql
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- HELPER FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------

create or replace PROCEDURE GATHER_TABLE_STATS(table_name VARCHAR2) AS 
  BEGIN
  DBMS_STATS.GATHER_TABLE_STATS (
          ownname => 'PCORNET_CDM', -- This doesn't work as a parameter for some reason.
          tabname => table_name,
          estimate_percent => 50, -- Percentage picked somewhat arbitrarily
          cascade => TRUE,
          degree => 16 
          );
END GATHER_TABLE_STATS;
/


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


--ACK: http://dba.stackexchange.com/questions/9441/how-to-catch-and-handle-only-specific-oracle-exceptions
create or replace procedure create_error_table(table_name varchar2) as
sqltext varchar2(4000); 

begin
  dbms_errlog.create_error_log(dml_table_name => table_name);
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE = -955 THEN
      NULL; -- suppresses ORA-00955 exception ("name is already used by an existing object")
    ELSE
       RAISE;
    END IF;
-- Delete rows from a previous run in case the table already existed
sqltext := 'delete from ERR$_' || table_name;
PMN_Execuatesql(sqltext);
end;
/


create or replace FUNCTION GETDATAMARTID RETURN VARCHAR2 IS 
BEGIN 
    RETURN '&&datamart_id';
END;
/


CREATE OR REPLACE FUNCTION GETDATAMARTNAME RETURN VARCHAR2 AS 
BEGIN 
    RETURN '&&datamart_name';
END;
/


CREATE OR REPLACE FUNCTION GETDATAMARTPLATFORM RETURN VARCHAR2 AS 
BEGIN 
    RETURN '02'; -- 01 is MSSQL, 02 is Oracle
END;
/


--------------------------------------------------------------------------------
-- I2B2 SYNONYMS, VIEWS, AND INTERMEDIARY TABLES
--------------------------------------------------------------------------------


CREATE OR REPLACE SYNONYM I2B2FACT FOR "&&i2b2_data_schema".OBSERVATION_FACT
/

CREATE OR REPLACE SYNONYM I2B2MEDFACT FOR OBSERVATION_FACT_MEDS
/

BEGIN
PMN_DROPSQL('DROP TABLE i2b2patient_list');
END;
/

CREATE table i2b2patient_list as 
select * from
(
select DISTINCT PATIENT_NUM from I2B2FACT where START_DATE > to_date('&&min_pat_list_date_dd_mon_rrrr','dd-mon-rrrr')
) where ROWNUM<100000000
/

create or replace VIEW i2b2patient as select * from "&&i2b2_data_schema".PATIENT_DIMENSION where PATIENT_NUM in (select PATIENT_NUM from i2b2patient_list)
/

create or replace view i2b2visit as select * from "&&i2b2_data_schema".VISIT_DIMENSION where START_DATE >= to_date('&&min_visit_date_dd_mon_rrrr','dd-mon-rrrr') and (END_DATE is NULL or END_DATE < CURRENT_DATE) and (START_DATE <CURRENT_DATE)
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


--------------------------------------------------------------------------------
-- I2P REPORT
--------------------------------------------------------------------------------

/* TODO: Consider building the loyalty cohort as designed: 
https://github.com/njgraham/SCILHS-utils/blob/master/LoyaltyCohort/LoyaltyCohort-ora.sql

For now, let's count all patients for testing with the KUMC test patients.
*/

--create or replace view i2b2loyalty_patients as (select patient_num,to_date('01-Jul-2010','dd-mon-rrrr') period_start,to_date('01-Jul-2014','dd-mon-rrrr') period_end from "&&i2b2_data_schema".loyalty_cohort_patient_summary where BITAND(filter_set, 61511) = 61511 and patient_num in (select patient_num from i2b2patient))
--/

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
select 'HISPANIC',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%';

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


--------------------------------------------------------------------------------
-- DEMOGRAPHIC
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE demographic');
END;
/
CREATE TABLE demographic(
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


--------------------------------------------------------------------------------
-- ENROLLMENT
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE enrollment');
END;
/
CREATE TABLE enrollment (
	PATID varchar(50) NOT NULL,
	ENR_START_DATE date NOT NULL,
	ENR_END_DATE date NULL,
	CHART varchar(1) NULL,
	ENR_BASIS varchar(1) NOT NULL,
	RAW_CHART varchar(50) NULL,
	RAW_BASIS varchar(50) NULL
)
/


--------------------------------------------------------------------------------
-- ENCOUNTER
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE encounter');
END;
/
CREATE TABLE encounter(
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
PMN_DROPSQL('DROP TABLE drg');
END;
/
CREATE TABLE drg (
  PATIENT_NUM NUMBER(38) NOT NULL, 
  ENCOUNTER_NUM NUMBER(38) NOT NULL, 
  DRG_TYPE VARCHAR2(2), 
  DRG VARCHAR2(3), 
  RN NUMBER
)
/


--------------------------------------------------------------------------------
-- DIAGNOSIS
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE diagnosis');
END;
/
CREATE TABLE diagnosis(
	DIAGNOSISID varchar(19)  primary key,
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
PMN_DROPSQL('DROP sequence  diagnosis_seq');
END;
/
create sequence  diagnosis_seq
/

create or replace trigger diagnosis_trg
before insert on diagnosis
for each row
begin
  select diagnosis_seq.nextval into :new.DIAGNOSISID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE sourcefact');
END;
/

CREATE TABLE SOURCEFACT  ( 
	PATIENT_NUM  	NUMBER(38) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	START_DATE   	DATE NOT NULL,
	DXSOURCE     	VARCHAR2(50) NULL,
	C_FULLNAME   	VARCHAR2(700) NOT NULL 
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE pdxfact');
END;
/

CREATE TABLE PDXFACT  ( 
	PATIENT_NUM  	NUMBER(38) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	START_DATE   	DATE NOT NULL,
	PDXSOURCE    	VARCHAR2(50) NULL,
	C_FULLNAME   	VARCHAR2(700) NOT NULL 
	)
/

--------------------------------------------------------------------------------
-- PROCEDURES
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE procedures');
END;
/
CREATE TABLE procedures(
	PROCEDURESID varchar(19)  primary key,
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
PMN_DROPSQL('DROP sequence  procedures_seq');
END;
/
create sequence  procedures_seq
/

create or replace trigger procedures_trg
before insert on procedures
for each row
begin
  select procedures_seq.nextval into :new.PROCEDURESID from dual;
end;
/


--------------------------------------------------------------------------------
-- VITAL
--------------------------------------------------------------------------------

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


--------------------------------------------------------------------------------
-- LAB_RESULT_CM
--------------------------------------------------------------------------------

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

-- TODO: This seems to belong in h2p-mapping
alter table "&&i2b2_meta_schema".pcornet_lab add (
  pcori_specimen_source varchar2(1000) -- arbitrary
  );


--------------------------------------------------------------------------------
-- CONDITION
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE condition');
END;
/
CREATE TABLE condition(
	CONDITIONID varchar(19)  primary key,
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
PMN_DROPSQL('DROP sequence  condition_seq');
END;
/
create sequence  condition_seq
/

create or replace trigger condition_trg
before insert on condition
for each row
begin
  select condition_seq.nextval into :new.CONDITIONID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE sourcefact2');
END;
/

CREATE TABLE SOURCEFACT2  ( 
	PATIENT_NUM  	NUMBER(38) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	START_DATE   	DATE NOT NULL,
	DXSOURCE     	VARCHAR2(50) NULL,
	C_FULLNAME   	VARCHAR2(700) NOT NULL 
	)
/


--------------------------------------------------------------------------------
-- PRO_CM
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE pro_cm');
END;
/
CREATE TABLE pro_cm(
	PRO_CM_ID varchar(19)  primary key,
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
PMN_DROPSQL('DROP sequence  pro_cm_seq');
END;
/
create sequence  pro_cm_seq
/

create or replace trigger pro_cm_trg
before insert on pro_cm
for each row
begin
  select pro_cm_seq.nextval into :new.PRO_CM_ID from dual;
end;
/


--------------------------------------------------------------------------------
-- PRESCRIBING
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE prescribing');
END;
/
CREATE TABLE prescribing(
	PRESCRIBINGID varchar(19)  primary key,
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
	RX_FREQUENCY varchar(2) NULL,
	RX_BASIS varchar (2) NULL,
	RXNORM_CUI int NULL,
	RAW_RX_MED_NAME varchar (50) NULL,
	RAW_RX_FREQUENCY varchar (50) NULL,
	RAW_RXNORM_CUI varchar (50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  prescribing_seq');
END;
/
create sequence  prescribing_seq
/

create or replace trigger prescribing_trg
before insert on prescribing
for each row
begin
  select prescribing_seq.nextval into :new.PRESCRIBINGID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE basis');
END;
/

CREATE TABLE BASIS  ( 
	PCORI_BASECODE	VARCHAR2(50) NULL,
	C_FULLNAME    	VARCHAR2(700) NOT NULL,
	INSTANCE_NUM  	NUMBER(18) NOT NULL,
	START_DATE    	DATE NOT NULL,
	PROVIDER_ID   	VARCHAR2(50) NOT NULL,
	CONCEPT_CD    	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM 	NUMBER(38) NOT NULL,
	MODIFIER_CD   	VARCHAR2(100) NOT NULL 
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE freq');
END;
/

CREATE TABLE FREQ  ( 
	PCORI_BASECODE	VARCHAR2(50) NULL,
	INSTANCE_NUM  	NUMBER(18) NOT NULL,
	START_DATE    	DATE NOT NULL,
	PROVIDER_ID   	VARCHAR2(50) NOT NULL,
	CONCEPT_CD    	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM 	NUMBER(38) NOT NULL,
	MODIFIER_CD   	VARCHAR2(100) NOT NULL 
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE quantity');
END;
/

CREATE TABLE QUANTITY  ( 
	NVAL_NUM     	NUMBER(18,5) NULL,
	INSTANCE_NUM 	NUMBER(18) NOT NULL,
	START_DATE   	DATE NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	MODIFIER_CD  	VARCHAR2(100) NOT NULL 
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE refills');
END;
/

CREATE TABLE REFILLS  ( 
	NVAL_NUM     	NUMBER(18,5) NULL,
	INSTANCE_NUM 	NUMBER(18) NOT NULL,
	START_DATE   	DATE NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	MODIFIER_CD  	VARCHAR2(100) NOT NULL 
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE supply');
END;
/

CREATE TABLE SUPPLY  ( 
	NVAL_NUM     	NUMBER(18,5) NULL,
	INSTANCE_NUM 	NUMBER(18) NOT NULL,
	START_DATE   	DATE NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	MODIFIER_CD  	VARCHAR2(100) NOT NULL 
	)
/


--------------------------------------------------------------------------------
-- DISPENSING
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE dispensing');
END;
/
CREATE TABLE dispensing(
	DISPENSINGID varchar(19)  primary key,
	PATID varchar(50) NOT NULL,
	PRESCRIBINGID varchar(19)  NULL,
	DISPENSE_DATE date NOT NULL,
	NDC varchar (11) NOT NULL,
	DISPENSE_SUP int, 
	DISPENSE_AMT int, 
	RAW_NDC varchar (50)
)
/

BEGIN
PMN_DROPSQL('DROP sequence  dispensing_seq');
END;
/
create sequence  dispensing_seq
/

create or replace trigger dispensing_trg
before insert on dispensing
for each row
begin
  select dispensing_seq.nextval into :new.DISPENSINGID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE DISP_SUPPLY');  --Changed the table 'supply' to the name 'disp_supply' to avoid conflicts with the prescribing procedure, Matthew Joss 8/16/16
END;
/

CREATE TABLE DISP_SUPPLY  ( 
	NVAL_NUM     	NUMBER(18,5) NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL 
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE amount');
END;
/

CREATE TABLE AMOUNT  ( 
	NVAL_NUM     	NUMBER(18,5) NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL 
	)
/

-- TODO: This seems to belong in h2p-mapping
alter table "&&i2b2_meta_schema".pcornet_med add (
  pcori_ndc varchar2(1000) -- arbitrary
  );


--------------------------------------------------------------------------------
-- PCORNET_TRIAL
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE pcornet_trial');
END;
/
CREATE TABLE pcornet_trial(
	PATID varchar(50) NOT NULL,
	TRIALID varchar(20) NOT NULL,
	PARTICIPANTID varchar(50) NOT NULL,
	TRIAL_SITEID varchar(50) NULL,
	TRIAL_ENROLL_DATE date NULL,
	TRIAL_END_DATE date NULL,
	TRIAL_WITHDRAW_DATE date NULL,
	TRIAL_INVITE_CODE varchar(20) NULL
)
/


--------------------------------------------------------------------------------
-- DEATH
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE death');
END;
/
CREATE TABLE death(
	PATID varchar(50) NOT NULL,
	DEATH_DATE date NOT NULL,
	DEATH_DATE_IMPUTE varchar(2) NULL,
	DEATH_SOURCE varchar(2) NOT NULL,
	DEATH_MATCH_CONFIDENCE varchar(2) NULL
)
/


--------------------------------------------------------------------------------
-- DEATH_CAUSE
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE death_cause');
END;
/
CREATE TABLE death_cause(
	PATID varchar(50) NOT NULL,
	DEATH_CAUSE varchar(8) NOT NULL,
	DEATH_CAUSE_CODE varchar(2) NOT NULL,
	DEATH_CAUSE_TYPE varchar(2) NOT NULL,
	DEATH_CAUSE_SOURCE varchar(2) NOT NULL,
	DEATH_CAUSE_CONFIDENCE varchar(2) NULL
)
/


--------------------------------------------------------------------------------
-- HARVEST
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE harvest');
END;
/
CREATE TABLE harvest(
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
	SPECIMEN_DATE_MGMT varchar(2) NULL,
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


