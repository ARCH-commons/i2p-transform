----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- PCORNetLoader Script
-- Oracle version by Kun Wei, kwei@wakehealth.edu
-- Version 5 by Jeff Klann, PhD; derived from version 4.1 and with contributions from Dan Connolly and Nathan Graham for vitals transformation
-- Version 4.1 by Aaron Abend, aaronabend@gmail.com, 978-621-7745
-- TODO: Code translation on encounter columns, unit conversion on vitals
-- 5.0 Now transforms vitals, enrollment (encounter basis), encounters (new columns and DRGs), speed improvements in diagnosis, bugfixes in procedures
-- 4.1 
-- 4.0 Splits HIB/HIW/etc codes into race and hispanic 
-- 3.8 Removed Duplicates due to invalid codes introduced by use of isnull
-- 3.6 Properly handle hispanic in the race_cd column
-- 3.5 Properly handle null sex and null race 
-- 3.4 fixed date formats in diagnosis, fixed extra/missing rows in diagnosis, removed unchecked drop statements
-- 3.3 fixes diagnoses to not miss unmapped PDX code
-- 3.2 fixes demographics - does not miss patients with unmapped sex or unmapped race
-- 3.1. puts admit diagnosis modifier in dx_source of pmn
-- MSSQL version
--
-- INSTRUCTIONS:
-- 1. Edit the "create synonym" statements and the USE statement at the top to point at your objects. 
--    This script will be run from the PopMedNet database you created.
-- 2. USE that new database and make sure it has privileges to read from the various locations that the synonyms point to.
-- 3. Run this script to set up pcornetloader
-- 4. Use the included run_*.sql script to execute the procedure, or run manually via "exec PCORNetLoader" (will transform all patients)

----------------------------------------------------------------------------------------------------------------------------------------
-- create synonyms to make the code portable - please edit these
----------------------------------------------------------------------------------------------------------------------------------------

use pmn;
/

-- drop any existing synonyms
--IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'i2b2fact') DROP SYNONYM i2b2fact
--IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'i2b2patient') DROP SYNONYM  i2b2patient
--IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'i2b2visit') DROP SYNONYM  i2b2visit
--IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pcornet_diag') DROP SYNONYM pcornet_diag
--IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pcornet_demo') DROP SYNONYM pcornet_demo
--IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pcornet_proc') DROP SYNONYM pcornet_proc

-- You will almost certainly need to edit your database name
create or replace synonym i2b2visit for i2b2demodata.visit_dimension ;
create or replace synonym i2b2patient for  i2b2demodata.patient_dimension;
create or replace synonym i2b2fact for  i2b2demodata.observation_fact    ;

-- You will almost certainly need to edit your database name
create or replace synonym pcornet_diag for i2b2metadata.pcornet_diag ;
create or replace synonym pcornet_demo for i2b2metadata.pcornet_demo ;
create or replace synonym pcornet_proc for i2b2metadata.pcornet_proc;
create or replace synonym pcornet_vital for i2b2metadata.pcornet_vital;
create or replace synonym pcornet_enc for i2b2metadata.pcornet_enc;


DROP TABLE IF  EXISTS pcornet_codelist;
/

create table pcornet_codelist (codetype varchar(20), code varchar(50));
/

----------------------------------------------------------------------------------------------------------------------------------------
-- CREATE THE TABLES - note that pmndemographic and pmnvital have changed since v4.1
----------------------------------------------------------------------------------------------------------------------------------------

/****** Object:  Table [dbo].[pmnENROLLMENT]    Script Date: 10/02/2014 15:59:37 ******/
DROP TABLE IF  EXISTS pmnenrollment;
/
CREATE TABLE pmnENROLLMENT(
	PATID varchar(50) NULL,
	ENR_START_DATE varchar(10) NULL,
	ENR_END_DATE varchar(10) NULL,
	CHART varchar(1) NULL,
	BASIS varchar(1) NULL,
	RAW_CHART varchar(50) NULL,
	RAW_BASIS varchar(50) NULL
);
/

/****** Object:  Table [dbo].[pmnVITAL]    Script Date: 10/02/2014 15:59:37 ******/

DROP TABLE IF  EXISTS pmnvital;
/

CREATE TABLE pmnVITAL(
	PATID varchar(50) NULL,
	ENCOUNTERID varchar(50) NULL,
	MEASURE_DATE varchar(10) NULL,
	MEASURE_TIME varchar(5) NULL,
	VITAL_SOURCE varchar(2) NULL,
    	HT number(8, 0) NULL,
	WT number(8, 0) NULL,
	DIASTOLIC number(4, 0) NULL,
	SYSTOLIC number(4, 0) NULL,
    	ORIGINAL_BMI number(8,0) NULL,
	BP_POSITION varchar(2) NULL,
	RAW_VITAL_SOURCE varchar(50) NULL,
	RAW_HT varchar(50) NULL,
	RAW_WT varchar(50) NULL,
	RAW_DIASTOLIC varchar(50) NULL,
	RAW_SYSTOLIC varchar(50) NULL,
	RAW_BP_POSITION varchar(50) NULL
);
/

/****** Object:  Table [dbo].[pmnPROCEDURE]    Script Date: 10/02/2014 15:59:37 ******/

DROP TABLE IF  EXISTS pmnprocedure;
/

CREATE TABLE pmnPROCEDURE(
	PATID varchar(50) NULL,
	ENCOUNTERID varchar(50) NULL,
	ENC_TYPE varchar(2) NULL,
	ADMIT_DATE varchar(10) NULL,
	PROVIDERID varchar(50) NULL,
	PX varchar(11) NULL,
	PX_TYPE varchar(2) NULL,
	RAW_PX varchar(50) NULL,
	RAW_PX_TYPE varchar(50) NULL
);
/

/****** Object:  Table [dbo].[pmndiagnosis]    Script Date: 10/02/2014 15:59:37 ******/

DROP TABLE IF  EXISTS pmndiagnosis;
/

CREATE TABLE pmndiagnosis(
	PATID varchar(50) NULL,
	ENCOUNTERID varchar(50) NULL,
	ENC_TYPE varchar(2) NULL,
	ADMIT_DATE varchar(10) NULL,
	PROVIDERID varchar(50) NULL,
	DX varchar(18) NULL,
	DX_TYPE varchar(2) NULL,
	DX_SOURCE varchar(2) NULL,
	PDX varchar(2) NULL,
	RAW_DX varchar(50) NULL,
	RAW_DX_TYPE varchar(50) NULL,
	RAW_DX_SOURCE varchar(50) NULL,
	RAW_ORIGDX varchar(50) NULL,
	RAW_PDX varchar(50) NULL
) ;
/
/****** Object:  Table [dbo].[pmnENCOUNTER]    Script Date: 10/02/2014 15:59:37 ******/

DROP TABLE IF  EXISTS pmnencounter;
/

CREATE TABLE pmnENCOUNTER(
	PATID varchar(50) NULL,
	ENCOUNTERID varchar(50) NOT NULL,
	ADMIT_DATE varchar(10) NULL,
	ADMIT_TIME varchar(5) NULL,
	DISCHARGE_DATE varchar(10) NULL,
	DISCHARGE_TIME varchar(5) NULL,
	PROVIDERID varchar(50) NULL,
	FACILITY_LOCATION varchar(3) NULL,
	ENC_TYPE varchar(2) NULL,
	FACILITYID varchar(50) NULL,
	DISCHARGE_DISPOSITION varchar(2) NULL,
	DISCHARGE_STATUS varchar(2) NULL,
	DRG varchar(3) NULL,
	DRG_TYPE varchar(2) NULL,
	ADMITTING_SOURCE varchar(2) NULL,
	RAW_ENC_TYPE varchar(50) NULL,
	RAW_DISCHARGE_DISPOSITION varchar(50) NULL,
	RAW_DISCHARGE_STATUS varchar(50) NULL,
	RAW_DRG_TYPE varchar(50) NULL,
	RAW_ADMITTING_SOURCE varchar(50) NULL
);
/
/****** Object:  Table [dbo].[pmndemographic]    Script Date: 10/02/2014 15:59:37 ******/

DROP TABLE IF  EXISTS pmndemographic;
/

CREATE TABLE pmndemographic(
	PATID varchar(50) NOT NULL,
	BIRTH_DATE varchar(10) NULL,
	BIRTH_TIME varchar(5) NULL,
	SEX varchar(2) NULL,
	HISPANIC varchar(2) NULL,
    	BIOBANK_FLAG varchar(1),
	RACE varchar(2) NULL,
	RAW_SEX varchar(50) NULL,
	RAW_HISPANIC varchar(50) NULL,
	RAW_RACE varchar(50) NULL
) ;
/

--ALTER TABLE dbo.pmndemographic
--ADD CONSTRAINT biobank_default
--DEFAULT 'N' FOR BIOBANK_FLAG;
--GO
--/****** Object:  ForeignKey [FK__pmndiagno__ENCOU__0AD2A005]    Script Date: 10/02/2014 15:59:37 ******/
--ALTER TABLE [dbo].[pmndiagnosis]  WITH CHECK ADD FOREIGN KEY([ENCOUNTERID])
--REFERENCES [dbo].[pmnENCOUNTER] ([ENCOUNTERID])
--GO
--/****** Object:  ForeignKey [FK__pmnENCOUN__PATID__07020F21]    Script Date: 10/02/2014 15:59:37 ******/
--ALTER TABLE [dbo].[pmnENCOUNTER]  WITH CHECK ADD FOREIGN KEY([PATID])
--REFERENCES [dbo].[pmndemographic] ([PATID])
--GO
--/****** Object:  ForeignKey [FK__pmnENROLL__PATID__023D5A04]    Script Date: 10/02/2014 15:59:37 ******/
--ALTER TABLE [dbo].[pmnENROLLMENT]  WITH CHECK ADD FOREIGN KEY([PATID])
--REFERENCES [dbo].[pmndemographic] ([PATID])
--GO
--/****** Object:  ForeignKey [FK__pmnPROCED__ENCOU__0CBAE877]    Script Date: 10/02/2014 15:59:37 ******/
--ALTER TABLE [dbo].[pmnPROCEDURE]  WITH CHECK ADD FOREIGN KEY([ENCOUNTERID])
--REFERENCES [dbo].[pmnENCOUNTER] ([ENCOUNTERID])
--GO
--/****** Object:  ForeignKey [FK__pmnVITAL__ENCOUN__08EA5793]    Script Date: 10/02/2014 15:59:37 ******/
--ALTER TABLE [dbo].[pmnVITAL]  WITH CHECK ADD FOREIGN KEY([ENCOUNTERID])
--REFERENCES [dbo].[pmnENCOUNTER] ([ENCOUNTERID])
--GO


----------------------------------------------------------------------------------------------------------------------------------------
-- Prep-to-transform code
----------------------------------------------------------------------------------------------------------------------------------------



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


-- create the reporting table - don't do this once you are running stuff and you want to track loads
DROP TABLE IF EXISTS i2pReport
/
create table i2pReport (runid number, rundate date, concept varchar(20), sourceval number, destval number, diff number)
/
insert into i2preport (runid) values (0)
/


-- Run the popcodelist procedure we just created
pcornet_popcodelist
/

--- Load the procedures

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 1. Demographics - v4.1 by Aaron Abend
--
----------------------------------------------------------------------------------------------------------------------------------------


create or replace procedure PCORNetDemographic as 

sqltext varchar2(4000); 
cursor getsql is 
--1 --  S,R,NH
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '||
	'	select ''1'',patient_num, '||
	'	to_char(birth_date,''YYYY-MM-DD''), '||
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
	'	to_char(birth_date,''YYYY-MM-DD''), '||
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
	'	to_char(birth_date,''YYYY-MM-DD''), '||
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
	'	to_char(birth_date,''YYYY-MM-DD''), '||
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
	'	to_char(birth_date,''YYYY-MM-DD''), '||
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
	'	to_char(birth_date,''YYYY-MM-DD''), '||
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
	'	to_char(birth_date,''YYYY-MM-DD''), '||
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
	'	to_char(birth_date,''YYYY-MM-DD''), '||
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


----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 2. Encounter - v5 by Jeff Klann and Aaron Abend
-- TODO: This version does not translate codes in the visit_dimension columns, except inout_cd (enc_type)
----------------------------------------------------------------------------------------------------------------------------------------


create or replace procedure PCORNetEncounter as

sqltext varchar2(4000);
begin

insert into pmnencounter(PATID,ENCOUNTERID,admit_date ,ADMIT_TIME , 
		DISCHARGE_DATE ,DISCHARGE_TIME ,PROVIDERID ,FACILITY_LOCATION  
		,ENC_TYPE ,FACILITYID ,DISCHARGE_DISPOSITION , 
		DISCHARGE_STATUS ,DRG ,DRG_TYPE ,ADMITTING_SOURCE) 
select distinct v.patient_num, v.encounter_num,  
	to_char(start_Date,'YYYY-MM-DD'), 
	to_char(start_Date,'HH:MI'), 
	to_char(end_Date,'YYYY-MM-DD'), 
	to_char(end_Date,'HH:MI'),  
	providerid,location_zip, 
(case when pcori_enctype is not null then pcori_enctype else 'UN' end) enc_type, facilityid, discharge_disposition, discharge_status, drg.drg, drg_type, admitting_source
from i2b2visit v inner join pmndemographic d on v.patient_num=d.patid
left outer join 
   (select patient_num,encounter_num,drg_type,max(drg) drg from
    (select distinct f.patient_num,encounter_num,SUBSTR(c_fullname,22,2) drg_type,SUBSTR(pcori_basecode,INSTR(pcori_basecode, ':')+1,3) drg from i2b2fact f 
     inner join pmndemographic d on f.patient_num=d.patid
     inner join pcornet_enc enc on enc.c_basecode  = f.concept_cd   
      and enc.c_fullname like '\PCORI\ENCOUNTER\DRG\%') drg1 group by patient_num,encounter_num,drg_type) drg
  on drg.patient_num=v.patient_num and drg.encounter_num=v.encounter_num
left outer join 
-- Encounter type. Note that this requires a full table scan on the ontology table, so it is not particularly efficient.
(select patient_num, encounter_num, inout_cd,SUBSTR(pcori_basecode,INSTR(pcori_basecode, ':')+1,2) pcori_enctype from i2b2visit v
 inner join pcornet_enc e on c_dimcode like '%'''|| inout_cd ||'''%' and e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%') enctype
  on enctype.patient_num=v.patient_num and enctype.encounter_num=v.encounter_num;

end PCORNetEncounter;
/

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 3. Diagnosis - v5 by Aaron Abend and Jeff Klann
----------------------------------------------------------------------------------------------------------------------------------------

create or replace procedure PCORNetDiagnosis as
sqltext varchar2(4000);
begin

-- Only updates the factline cache if it doesn't exist
sqltext := 'create global temporary table factline on commit delete rows as' || 
 'select distinct patient_num, encounter_num, f.provider_id, f.concept_cd, f.start_date,modifier_cd,enc.enc_type ' ||
 'from i2b2fact f,pmnENCOUNTER enc where enc.patid = f.patient_num and enc.encounterid = f.encounter_Num';
execute immediate  sqltext;

execute immediate 'DROP VIEW IF  EXISTS sourcefact;';
sqltext := 'create view sourcefact as '||
	'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode dxsource ' ||
	'from factline, pcornet_diag dxsource where factline.modifier_cd =dxsource.c_basecode '||
	'and dxsource.c_fullname like ''\PCORI_MOD\DX_SOURCE\%''';
execute immediate sqltext;

execute immediate 'DROP VIEW IF EXISTS pdxfact';
sqltext := 'create view pdxfact as '||
	'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode pdxsource ' ||
	'from factline, pcornet_diag dxsource where factline.modifier_cd =dxsource.c_basecode '||
	'and dxsource.c_fullname like ''\PCORI_MOD\PDX\%''';
execute immediate sqltext;

execute immediate  'insert into pmndiagnosis (patid,			encounterid,	enc_type, admit_date, providerid, dx, dx_type, dx_source, pdx)
select distinct factline.patient_num, factline.encounter_num encounterid,	enc_type, to_char(factline.start_date,''YYYY-MM-DD''), factline.provider_id, diag.pcori_basecode, 
SUBSTR(diag.c_fullname,18,2) dxtype,  
	nvl(SUBSTR(dxsource,INSTR(dxsource,'':'')+1,2) ,''NI''),
	nvl(SUBSTR(pdxsource,INSTR(pdxsource, '':'')+1,2),''NI'')
from factline left outer join sourcefact
on	factline.patient_num=sourcefact.patient_num
and factline.encounter_num=sourcefact.encounter_num
and factline.provider_id=sourcefact.provider_id
and factline.concept_cd=sourcefact.concept_Cd
and factline.start_date=sourcefact.start_Date 
left outer join pdxfact
on	factline.patient_num=pdxfact.patient_num
and factline.encounter_num=pdxfact.encounter_num
and factline.provider_id=pdxfact.provider_id
and factline.concept_cd=pdxfact.concept_cd
and factline.start_date=pdxfact.start_Date,
	pcornet_diag diag	
where diag.c_basecode  = factline.concept_cd   
and diag.c_fullname like ''\PCORI\DIAGNOSIS\%''';

execute immediate 'DROP table factline;';

end PCORNetDiagnosis;
/


----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 4. Procedures - v5 by Aaron Abend and Jeff Klann
----------------------------------------------------------------------------------------------------------------------------------------


create or replace procedure PCORNetProcedure as

begin
insert into pmnprocedure( 
				patid,			encounterid,	enc_type, admit_date, providerid, px, px_type) 
select  distinct fact.patient_num, enc.encounterid,	enc.enc_type, to_char(fact.start_date,'YYYY-MM-DD'), 
		fact.provider_id, SUBSTR(pr.pcori_basecode,INSTR(pr.pcori_basecode, ':')+1,11) px, SUBSTR(pr.c_fullname,18,2) pxtype 
from i2b2fact fact, 
	pcornet_proc pr, 
	pmnencounter enc 
where pr.c_basecode  = fact.concept_cd   
and pr.c_fullname like '\PCORI\PROCEDURE\%'
and enc.encounterid = fact.encounter_Num;

end PCORNetProcedure;
/

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 5. Vitals - v5 by Jeff Klann
-- TODO: This version does not do unit conversions.
----------------------------------------------------------------------------------------------------------------------------------------
------------------------- Vitals Code ------------------------------------------------ 
-- 12/15/14
-- Written by Jeff Klann, PhD
-- Borrows heavily from GPC's CDM_transform.sql by Dan Connolly and Nathan Graham, available at https://bitbucket.org/njgraham/pcori-annotated-data-dictionary/overview 
-- This transform must be run after demographics and encounters. It does not rely on the PCORI_basecode column, so that does not need to be changed.
-- TODO: This does not do unit conversions.


create or replace procedure PCORNetVital as
begin
-- jgk: I took out admit_date - it doesn't appear in the scheme. Now in SQLServer format - date, substring, name on inner select, no nested with. Added modifiers and now use only pathnames, not codes.
-- TODO: There is no data for the modifiers. Needs testing.
insert into pmnVITAL(patid, encounterid, measure_date, measure_time,vital_source,ht, wt, diastolic, systolic, original_bmi, bp_position) 
select patid, encounterid, measure_date, measure_time, nvl(max(vital_source),'NI') vital_source, -- jgk: not in the spec, so I took it out  admit_date,
max(ht) ht, max(wt) wt, max(diastolic) diastolic, max(systolic) systolic, 
max(original_bmi) original_bmi, nvl(max(bp_position),'NI') bp_position
from (
  select vit.patid, vit.encounterid, vit.measure_date, vit.measure_time 
    , case when vit.pcori_code like '\PCORI\VITAL\HT%' then vit.nval_num else null end ht
    , case when vit.pcori_code like '\PCORI\VITAL\WT%' then vit.nval_num else null end wt
    , case when vit.pcori_code like '\PCORI\VITAL\BP\DIASTOLIC%' then vit.nval_num else null end diastolic
    , case when vit.pcori_code like '\PCORI\VITAL\BP\SYSTOLIC%' then vit.nval_num else null end systolic
    , case when vit.pcori_code like '\PCORI\VITAL\ORIGINAL_BMI%' then vit.nval_num else null end original_bmi
    , case when vit.pcori_code like '\PCORI_MOD\BP_POSITION\%' then SUBSTR(vit.pcori_code,LENGTH(vit.pcori_code)-2,2) else null end bp_position
    , case when vit.pcori_code like '\PCORI_MOD\VITAL_SOURCE\%' then SUBSTR(vit.pcori_code,LENGTH(vit.pcori_code)-2,2) else null end vital_source
    , enc.admit_date
  from pmndemographic pd
  left join (
    select 
      obs.patient_num patid, obs.encounter_num encounterid, 
      	to_char(obs.start_Date,'YYYY-MM-DD') measure_date, 
	to_char(obs.start_Date,'HH:MI') measure_time, 
      nval_num, codes.pcori_code
    from i2b2fact obs
    inner join (select c_basecode concept_cd, c_fullname pcori_code
      from (
        select '\PCORI\VITAL\BP\DIASTOLIC\' concept_path FROM DUAL
        union all
        select '\PCORI\VITAL\BP\SYSTOLIC\' concept_path FROM DUAL
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
group by patid, encounterid, measure_date, measure_time, admit_date;

end PCORNetVital;
/

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 6. Enrollment - v5 by Jeff Klann
----------------------------------------------------------------------------------------------------------------------------------------
------------------------- Enrollment Code ------------------------------------------------ 
-- 12/15/14
-- Written by Jeff Klann, PhD
-- This only suppports encounter basis right now. Should switch to algorithmic when Griffin's approach is finalized.

create or replace procedure  PCORNetEnroll as
begin

INSERT INTO pmnENROLLMENT(PATID, ENR_START_DATE, ENR_END_DATE, CHART, BASIS) 
    select patient_num patid, to_char(enr_start,'YYYY-MM-DD') enr_start_date
    , case when enr_end_end>enr_end then to_char(enr_end_end,'YYYY-MM-DD') else to_char(enr_end,'YYYY-MM-DD') end enr_end_date 
    ,'Y' chart, 'E' basis from 
    (select patient_num, min(start_date) enr_start,max(start_date) enr_end,max(end_date) enr_end_end from i2b2fact where patient_num in (select patid from pmndemographic) group by patient_num) x;


end PCORNetEnroll;
/


----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 7. clear Program - includes all tables
----------------------------------------------------------------------------------------------------------------------------------------

create or replace procedure pcornetclear
as
begin


DELETE FROM pmnprocedure;
DELETE FROM pmndiagnosis;
DELETE FROM pmnvital;
DELETE FROM pmnenrollment;
DELETE FROM pmnencounter;
DELETE FROM pmndemographic;

end pcornetclear;
/

----------------------------------------------------------------------------------------------------------------------------------------
-- 8. Load Program
----------------------------------------------------------------------------------------------------------------------------------------

create or replace procedure pcornetloader
as
begin
pcornetclear;
PCORNetDemographic;
PCORNetEncounter;
PCORNetDiagnosis;
PCORNetProcedure;
PCORNetVital;
PCORNetEnroll;
pcornetreport;

end pcornetloader;
/


----------------------------------------------------------------------------------------------------------------------------------------
-- 9. Report Results - Version 5 by Aaron Abend and Jeff Klann
-- This version is useful to check against i2b2, but consider running the more detailed annotated data dictionary tool also.
----------------------------------------------------------------------------------------------------------------------------------------

create or replace procedure pcornetReport 
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

v_runid number;
begin

select count(*) into i2b2Pats   from i2b2patient;
select count(*) into i2b2Encounters   from i2b2visit i inner join pmndemographic d on i.patient_num=d.patid;
---select @i2b2Facts=count(*)   from i2b2fact where concept_Cd like 'ICD9%'

select count(*) into pmnPats   from pmndemographic;
select count(*) into pmnencounters   from pmnencounter e ;
select count(*) into pmndx   from pmndiagnosis;
select count(*) into pmnprocs  from pmnprocedure;
select count(*) into pmnenroll  from pmnenrollment;
select count(*) into pmnvital  from pmnvital;

select max(runid) into v_runid from i2pReport;
v_runid := v_runid + 1;
insert into i2pReport values( v_runid, SYSDATE, 'Pats', i2b2pats, pmnpats, i2b2pats-pmnpats);
insert into i2pReport values( v_runid, SYSDATE, 'Enrollment', i2b2pats, pmnenroll, i2b2pats-pmnpats);

insert into i2pReport values(v_runid, SYSDATE, 'Encounters', i2b2Encounters, pmnEncounters, i2b2encounters-pmnencounters);
insert into i2pReport values( v_runid, SYSDATE, 'DXPX', null, pmndx+pmnprocs, null);
insert into i2pReport values( v_runid, SYSDATE, 'Vital', null, pmnvital, null);

--select concept 'Data Type',sourceval 'From i2b2',destval 'In PopMedNet', diff 'Difference' from i2preport where runid=v_runid;

end pcornetReport;