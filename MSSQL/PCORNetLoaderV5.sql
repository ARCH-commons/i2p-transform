----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- PCORNetLoader Script
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

use pmn
go

-- drop any existing synonyms
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'i2b2fact') DROP SYNONYM i2b2fact
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'i2b2patient') DROP SYNONYM  i2b2patient
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'i2b2visit') DROP SYNONYM  i2b2visit
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pcornet_diag') DROP SYNONYM pcornet_diag
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pcornet_demo') DROP SYNONYM pcornet_demo
IF  EXISTS (SELECT * FROM sys.synonyms WHERE name = N'pcornet_proc') DROP SYNONYM pcornet_proc

-- You will almost certainly need to edit your database name
create synonym i2b2visit for i2b2demodata..visit_dimension 
create synonym i2b2patient for  i2b2demodata..patient_dimension
create synonym i2b2fact for  i2b2demodata..observation_fact    

-- You will almost certainly need to edit your database name
create synonym pcornet_diag for i2b2metadata..pcornet_diag 
create synonym pcornet_demo for i2b2metadata..pcornet_demo 
create synonym pcornet_proc for i2b2metadata..pcornet_proc
create synonym pcornet_vital for i2b2metadata..pcornet_vital
create synonym pcornet_enc for i2b2metadata..pcornet_enc

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pcornet_codelist]') AND type in (N'U'))
DROP TABLE [dbo].[pcornet_codelist]
GO

create table pcornet_codelist (codetype varchar(20), code varchar(20))
go

----------------------------------------------------------------------------------------------------------------------------------------
-- CREATE THE TABLES - note that pmndemographic and pmnvital have changed since v4.1
----------------------------------------------------------------------------------------------------------------------------------------

/****** Object:  Table [dbo].[pmnENROLLMENT]    Script Date: 10/02/2014 15:59:37 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pmnenrollment]') AND type in (N'U'))
DROP TABLE [dbo].[pmnenrollment]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[pmnENROLLMENT](
	[PATID] [varchar](50) NULL,
	[ENR_START_DATE] [varchar](10) NULL,
	[ENR_END_DATE] [varchar](10) NULL,
	[CHART] [varchar](1) NULL,
	[BASIS] [varchar](1) NULL,
	[RAW_CHART] [varchar](50) NULL,
	[RAW_BASIS] [varchar](50) NULL
) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[pmnVITAL]    Script Date: 10/02/2014 15:59:37 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pmnvital]') AND type in (N'U'))
DROP TABLE [dbo].[pmnvital]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[pmnVITAL](
	[PATID] [varchar](50) NULL,
	[ENCOUNTERID] [varchar](50) NULL,
	[MEASURE_DATE] [varchar](10) NULL,
	[MEASURE_TIME] [varchar](5) NULL,
	[VITAL_SOURCE] [varchar](2) NULL,
    [HT] [numeric](8, 0) NULL,
	[WT] [numeric](8, 0) NULL,
	[DIASTOLIC] [numeric](4, 0) NULL,
	[SYSTOLIC] [numeric](4, 0) NULL,
    [ORIGINAL_BMI] [numeric](8,0) NULL,
	[BP_POSITION] [varchar](2) NULL,
	[RAW_VITAL_SOURCE] [varchar](50) NULL,
	[RAW_HT] [varchar](50) NULL,
	[RAW_WT] [varchar](50) NULL,
	[RAW_DIASTOLIC] [varchar](50) NULL,
	[RAW_SYSTOLIC] [varchar](50) NULL,
	[RAW_BP_POSITION] [varchar](50) NULL
) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[pmnPROCEDURE]    Script Date: 10/02/2014 15:59:37 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pmnprocedure]') AND type in (N'U'))
DROP TABLE [dbo].[pmnprocedure]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[pmnPROCEDURE](
	[PATID] [varchar](50) NULL,
	[ENCOUNTERID] [varchar](50) NULL,
	[ENC_TYPE] [varchar](2) NULL,
	[ADMIT_DATE] [varchar](10) NULL,
	[PROVIDERID] [varchar](50) NULL,
	[PX] [varchar](11) NULL,
	[PX_TYPE] [varchar](2) NULL,
	[RAW_PX] [varchar](50) NULL,
	[RAW_PX_TYPE] [varchar](50) NULL
) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[pmndiagnosis]    Script Date: 10/02/2014 15:59:37 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pmndiagnosis]') AND type in (N'U'))
DROP TABLE [dbo].[pmndiagnosis]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[pmndiagnosis](
	[PATID] [varchar](50) NULL,
	[ENCOUNTERID] [varchar](50) NULL,
	[ENC_TYPE] [varchar](2) NULL,
	[ADMIT_DATE] [varchar](10) NULL,
	[PROVIDERID] [varchar](50) NULL,
	[DX] [varchar](18) NULL,
	[DX_TYPE] [varchar](2) NULL,
	[DX_SOURCE] [varchar](2) NULL,
	[PDX] [varchar](2) NULL,
	[RAW_DX] [varchar](50) NULL,
	[RAW_DX_TYPE] [varchar](50) NULL,
	[RAW_DX_SOURCE] [varchar](50) NULL,
	[RAW_ORIGDX] [varchar](50) NULL,
	[RAW_PDX] [varchar](50) NULL
) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[pmnENCOUNTER]    Script Date: 10/02/2014 15:59:37 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pmnencounter]') AND type in (N'U'))
DROP TABLE [dbo].[pmnencounter]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[pmnENCOUNTER](
	[PATID] [varchar](50) NULL,
	[ENCOUNTERID] [varchar](50) NOT NULL,
	[ADMIT_DATE] [varchar](10) NULL,
	[ADMIT_TIME] [varchar](5) NULL,
	[DISCHARGE_DATE] [varchar](10) NULL,
	[DISCHARGE_TIME] [varchar](5) NULL,
	[PROVIDERID] [varchar](50) NULL,
	[FACILITY_LOCATION] [varchar](3) NULL,
	[ENC_TYPE] [varchar](2) NULL,
	[FACILITYID] [varchar](50) NULL,
	[DISCHARGE_DISPOSITION] [varchar](2) NULL,
	[DISCHARGE_STATUS] [varchar](2) NULL,
	[DRG] [varchar](3) NULL,
	[DRG_TYPE] [varchar](2) NULL,
	[ADMITTING_SOURCE] [varchar](2) NULL,
	[RAW_ENC_TYPE] [varchar](50) NULL,
	[RAW_DISCHARGE_DISPOSITION] [varchar](50) NULL,
	[RAW_DISCHARGE_STATUS] [varchar](50) NULL,
	[RAW_DRG_TYPE] [varchar](50) NULL,
	[RAW_ADMITTING_SOURCE] [varchar](50) NULL,
PRIMARY KEY CLUSTERED 
(
	[ENCOUNTERID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[pmndemographic]    Script Date: 10/02/2014 15:59:37 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pmndemographic]') AND type in (N'U'))
DROP TABLE [dbo].[pmndemographic]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[pmndemographic](
	[PATID] [varchar](50) NOT NULL,
	[BIRTH_DATE] [varchar](10) NULL,
	[BIRTH_TIME] [varchar](5) NULL,
	[SEX] [varchar](2) NULL,
	[HISPANIC] [varchar](2) NULL,
    [BIOBANK_FLAG] [varchar](1),
	[RACE] [varchar](2) NULL,
	[RAW_SEX] [varchar](50) NULL,
	[RAW_HISPANIC] [varchar](50) NULL,
	[RAW_RACE] [varchar](50) NULL,
PRIMARY KEY CLUSTERED 
(
	[PATID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
ALTER TABLE dbo.pmndemographic
ADD CONSTRAINT biobank_default
DEFAULT 'N' FOR BIOBANK_FLAG;
GO
/****** Object:  ForeignKey [FK__pmndiagno__ENCOU__0AD2A005]    Script Date: 10/02/2014 15:59:37 ******/
ALTER TABLE [dbo].[pmndiagnosis]  WITH CHECK ADD FOREIGN KEY([ENCOUNTERID])
REFERENCES [dbo].[pmnENCOUNTER] ([ENCOUNTERID])
GO
/****** Object:  ForeignKey [FK__pmnENCOUN__PATID__07020F21]    Script Date: 10/02/2014 15:59:37 ******/
ALTER TABLE [dbo].[pmnENCOUNTER]  WITH CHECK ADD FOREIGN KEY([PATID])
REFERENCES [dbo].[pmndemographic] ([PATID])
GO
/****** Object:  ForeignKey [FK__pmnENROLL__PATID__023D5A04]    Script Date: 10/02/2014 15:59:37 ******/
ALTER TABLE [dbo].[pmnENROLLMENT]  WITH CHECK ADD FOREIGN KEY([PATID])
REFERENCES [dbo].[pmndemographic] ([PATID])
GO
/****** Object:  ForeignKey [FK__pmnPROCED__ENCOU__0CBAE877]    Script Date: 10/02/2014 15:59:37 ******/
ALTER TABLE [dbo].[pmnPROCEDURE]  WITH CHECK ADD FOREIGN KEY([ENCOUNTERID])
REFERENCES [dbo].[pmnENCOUNTER] ([ENCOUNTERID])
GO
/****** Object:  ForeignKey [FK__pmnVITAL__ENCOUN__08EA5793]    Script Date: 10/02/2014 15:59:37 ******/
ALTER TABLE [dbo].[pmnVITAL]  WITH CHECK ADD FOREIGN KEY([ENCOUNTERID])
REFERENCES [dbo].[pmnENCOUNTER] ([ENCOUNTERID])
GO


----------------------------------------------------------------------------------------------------------------------------------------
-- Prep-to-transform code
----------------------------------------------------------------------------------------------------------------------------------------

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pcornet_parsecode]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[pcornet_parsecode]
GO

create procedure pcornet_parsecode (@codetype varchar(20), @codestring varchar(1000)) as

declare @tex varchar(2000)
declare @pos int
declare @readstate char(1) 
declare @nextchar char(1) 
declare @val varchar(20)

begin

set @val=''
set @readstate='F'
set @pos=0
set @tex = @codestring
while @pos<len(@tex)
begin
	set @pos = @pos +1
	set @nextchar=substring(@tex,@pos,1)
	if @nextchar=',' continue
	if @nextchar='''' 
	begin
		if @readstate='F' 
			begin
			set @readstate='T' 
			continue
			end
		else 
			begin
			insert into pcornet_codelist values (@codetype,@val)
			set @val=''
			set @readstate='F'  
			end
	end
	if @readstate='T'
	begin
		set @val= @val + @nextchar
	end		
end 
end
go

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pcornet_popcodelist]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[pcornet_popcodelist]
GO
create procedure pcornet_popcodelist as

declare @codedata varchar(2000)
declare @onecode varchar(20)
declare @codetype varchar(20)

declare getcodesql cursor local for
select 'RACE',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
union
select 'SEX',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
union
select 'HISPANIC',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC\Y%'

begin
delete from pcornet_codelist;
open getcodesql ;
fetch next from getcodesql  into @codetype,@codedata;
while @@fetch_status=0
begin	
 
	exec pcornet_parsecode  @codetype,@codedata 
	fetch next from getcodesql  into @codetype,@codedata;
end

close getcodesql ;
deallocate getcodesql ;
end

go

-- create the reporting table - don't do this once you are running stuff and you want to track loads
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'i2pReport') AND type in (N'U')) DROP TABLE i2pReport
GO
create table i2pReport (runid numeric, rundate smalldatetime, concept varchar(20), sourceval numeric, destval numeric, diff numeric)
go
insert into i2preport (runid) select 0

-- Run the popcodelist procedure we just created
EXEC pcornet_popcodelist
GO

--- Load the procedures

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 1. Demographics - v4.1 by Aaron Abend
--
----------------------------------------------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'PCORNetDemographics') AND type in (N'P', N'PC')) DROP PROCEDURE PCORNetDemographics
go

create procedure PCORNetDemographics as 

DECLARE @sqltext NVARCHAR(4000);
DECLARE @batchid numeric
declare getsql cursor local for 
--1 --  S,R,NH
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '+
	'	select ''1'',patient_num, '+
	'	substring(convert(varchar,birth_date,20),1,10), '+
	'	substring(convert(varchar,birth_date,20),12,5), '+
	''''+sex.pcori_basecode+''','+
	'''NI'','+
	''''+race.pcori_basecode+''''+
	' from i2b2patient p '+
	'	where lower(p.sex_cd) in ('+lower(sex.c_dimcode)+') '+
	'	and	lower(p.race_cd) in ('+lower(race.c_dimcode)+') '+
	'   and lower(isnull(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo race, pcornet_demo sex
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union -- A - S,R,H
select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '+
	'	select ''A'',patient_num, '+
	'	substring(convert(varchar,birth_date,20),1,10), '+
	'	substring(convert(varchar,birth_date,20),12,5), '+
	''''+sex.pcori_basecode+''','+
	''''+hisp.pcori_basecode+''','+
	''''+race.pcori_basecode+''''+
	' from i2b2patient p '+
	'	where lower(p.sex_cd) in ('+lower(sex.c_dimcode)+') '+
	'	and	lower(p.race_cd) in ('+lower(race.c_dimcode)+') '+
	'	and	lower(isnull(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''RACE'') '+
	'   and lower(isnull(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo race, pcornet_demo hisp, pcornet_demo sex
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC\Y%'
	and hisp.c_visualattributes like 'L%'
	and sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --2 S, nR, nH
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '+
	'	select ''2'',patient_num, '+
	'	substring(convert(varchar,birth_date,20),1,10), '+
	'	substring(convert(varchar,birth_date,20),12,5), '+
	''''+sex.pcori_basecode+''','+
	'''NI'','+
	'''NI'''+
	' from i2b2patient p '+
	'	where lower(isnull(p.sex_cd,''xx'')) in ('+lower(sex.c_dimcode)+') '+
	'	and	lower(isnull(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '+
	'   and lower(isnull(p.race_cd,''ni'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo sex
	where sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --3 -- nS,R, NH
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '+
	'	select ''3'',patient_num, '+
	'	substring(convert(varchar,birth_date,20),1,10), '+
	'	substring(convert(varchar,birth_date,20),12,5), '+
	'''NI'','+
	'''NI'','+
	''''+race.pcori_basecode+''''+
	' from i2b2patient p '+
	'	where lower(isnull(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '+
	'	and	lower(p.race_cd) in ('+lower(race.c_dimcode)+') '+
	'   and lower(isnull(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'')'
	from pcornet_demo race
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
union --B -- nS,R, H
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '+
	'	select ''B'',patient_num, '+
	'	substring(convert(varchar,birth_date,20),1,10), '+
	'	substring(convert(varchar,birth_date,20),12,5), '+
	'''NI'','+
	''''+hisp.pcori_basecode+''','+
	''''+race.pcori_basecode+''''+
	' from i2b2patient p '+
	'	where lower(isnull(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '+
	'	and	lower(p.race_cd) in ('+lower(race.c_dimcode)+') '+
	'	and	lower(isnull(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''RACE'') '+
	'   and lower(isnull(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'')'
	from pcornet_demo race,pcornet_demo hisp
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC\Y%'
	and hisp.c_visualattributes like 'L%'
union --4 -- S, NR, H
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '+
	'	select ''4'',patient_num, '+
	'	substring(convert(varchar,birth_date,20),1,10), '+
	'	substring(convert(varchar,birth_date,20),12,5), '+
	''''+sex.pcori_basecode+''','+
	'''Y'','+
	'''NI'''+
	' from i2b2patient p '+
	'	where lower(isnull(p.sex_cd,''NI'')) in ('+lower(sex.c_dimcode)+') '+
	'	and lower(isnull(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '+
	'	and lower(isnull(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo sex
	where sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --5 -- NS, NR, H
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '+
	'	select ''5'',patient_num, '+
	'	substring(convert(varchar,birth_date,20),1,10), '+
	'	substring(convert(varchar,birth_date,20),12,5), '+
	'''NI'','+
	'''Y'','+
	'''NI'''+
	' from i2b2patient p '+
	'	where lower(isnull(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '+
	'	and lower(isnull(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '+
	'	and lower(isnull(p.race_cd,''xx'')) in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'')'
union --6 -- NS, NR, nH
	select 'insert into PMNDEMOGRAPHIC(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, HISPANIC, RACE) '+
	'	select ''6'',patient_num, '+
	'	substring(convert(varchar,birth_date,20),1,10), '+
	'	substring(convert(varchar,birth_date,20),12,5), '+
	'''NI'','+
	'''NI'','+
	'''NI'''+
	' from i2b2patient p '+
	'	where lower(isnull(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '+
	'	and lower(isnull(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '+
	'   and lower(isnull(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') ' 

begin
exec pcornet_popcodelist

set @batchid = 0
OPEN getsql;
FETCH NEXT FROM getsql INTO @sqltext;

WHILE @@FETCH_STATUS = 0
BEGIN
	--print @sqltext
	exec sp_executesql @sqltext
	FETCH NEXT FROM getsql INTO @sqltext;
	
END

CLOSE getsql;
DEALLOCATE getsql;
end

go

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 2. Encounter - v5 by Jeff Klann and Aaron Abend
-- TODO: This version does not translate codes in the visit_dimension columns, except inout_cd (enc_type)
----------------------------------------------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'PCORNetEncounter') AND type in (N'P', N'PC'))
DROP PROCEDURE PCORNetEncounter
GO

create procedure PCORNetEncounter as

DECLARE @sqltext NVARCHAR(4000);
begin

insert into pmnencounter(PATID,ENCOUNTERID,admit_date ,ADMIT_TIME , 
		DISCHARGE_DATE ,DISCHARGE_TIME ,PROVIDERID ,FACILITY_LOCATION  
		,ENC_TYPE ,FACILITYID ,DISCHARGE_DISPOSITION , 
		DISCHARGE_STATUS ,DRG ,DRG_TYPE ,ADMITTING_SOURCE) 
select distinct v.patient_num, v.encounter_num,  
	substring(convert(varchar,start_Date,20),1,10), 
	substring(convert(varchar,start_Date,20),12,5), 
	substring(convert(varchar, end_Date,20),1,10), 
	substring(convert(varchar, end_Date,20),12,5),  
	providerid,location_zip, 
(case when pcori_enctype is not null then pcori_enctype else 'UN' end) enc_type, facilityid, discharge_disposition, discharge_status, drg.drg, drg_type, admitting_source
from i2b2visit v inner join pmndemographic d on v.patient_num=d.patid
left outer join 
   (select patient_num,encounter_num,drg_type,max(drg) drg from
    (select distinct f.patient_num,encounter_num,substring(c_fullname,22,2) drg_type,substring(pcori_basecode,charindex(':',pcori_basecode)+1,3) drg from i2b2fact f 
     inner join pmndemographic d on f.patient_num=d.patid
     inner join pcornet_enc enc on enc.c_basecode  = f.concept_cd   
      and enc.c_fullname like '\PCORI\ENCOUNTER\DRG\%') drg1 group by patient_num,encounter_num,drg_type) drg
  on drg.patient_num=v.patient_num and drg.encounter_num=v.encounter_num
left outer join 
-- Encounter type. Note that this requires a full table scan on the ontology table, so it is not particularly efficient.
(select patient_num, encounter_num, inout_cd,substring(pcori_basecode,charindex(':',pcori_basecode)+1,2) pcori_enctype from i2b2visit v
 inner join pcornet_enc e on c_dimcode like '%'''+inout_cd+'''%' and e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%') enctype
  on enctype.patient_num=v.patient_num and enctype.encounter_num=v.encounter_num

end
go

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 3. Diagnosis - v5 by Aaron Abend and Jeff Klann
----------------------------------------------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'PCORNetDiagnosis') AND type in (N'P', N'PC')) DROP PROCEDURE PCORNetDiagnosis
go

create procedure PCORNetDiagnosis as
declare @sqltext nvarchar(4000)
begin

-- Only updates the factline cache if it doesn't exist
set @sqltext=N'IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(''factline''))' + 
 N'select distinct patient_num, encounter_num, f.provider_id, f.concept_cd, f.start_date,modifier_cd,enc.enc_type ' +
 N'into factline from i2b2fact f,pmnENCOUNTER enc where enc.patid = f.patient_num and enc.encounterid = f.encounter_Num'
exec sp_executesql @sqltext

exec sp_executesql N'IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N''sourcefact'')) DROP VIEW sourcefact'
set @sqltext = N'create view sourcefact as '+
	N'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode dxsource '+ 
	N'from factline, pcornet_diag dxsource where factline.modifier_cd =dxsource.c_basecode '+
	N'and dxsource.c_fullname like ''\PCORI_MOD\DX_SOURCE\%'''
exec sp_executesql @sqltext

exec sp_executesql N'IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N''pdxfact'')) DROP VIEW pdxfact'
set @sqltext = N'create view pdxfact as '+
	N'select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode pdxsource '+ 
	N'from factline, pcornet_diag dxsource where factline.modifier_cd =dxsource.c_basecode '+
	N'and dxsource.c_fullname like ''\PCORI_MOD\PDX\%'''
exec sp_executesql @sqltext

insert into pmndiagnosis (patid,			encounterid,	enc_type, admit_date, providerid, dx, dx_type, dx_source, pdx)
select distinct factline.patient_num, factline.encounter_num encounterid,	enc_type, substring(convert(varchar,factline.start_date,20),1,10), factline.provider_id, diag.pcori_basecode, 
substring(diag.c_fullname,18,2) dxtype,  
	isnull(substring(dxsource,charindex(':',dxsource)+1,2) ,'NI'),
	isnull(substring(pdxsource,charindex(':',pdxsource)+1,2),'NI')
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
and diag.c_fullname like '\PCORI\DIAGNOSIS\%'

drop table factline

end
go

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 4. Procedures - v5 by Aaron Abend and Jeff Klann
----------------------------------------------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'PCORNetProcedure') AND type in (N'P', N'PC')) DROP PROCEDURE PCORNetProcedure
go

create procedure PCORNetProcedure as

begin
insert into pmnprocedure( 
				patid,			encounterid,	enc_type, admit_date, providerid, px, px_type) 
select  distinct fact.patient_num, enc.encounterid,	enc.enc_type, substring(convert(varchar,fact.start_date,20),1,10), 
		fact.provider_id, substring(pr.pcori_basecode,charindex(':',pr.pcori_basecode)+1,11) px, substring(pr.c_fullname,18,2) pxtype 
from i2b2fact fact, 
	pcornet_proc pr, 
	pmnencounter enc 
where pr.c_basecode  = fact.concept_cd   
and pr.c_fullname like '\PCORI\PROCEDURE\%'
and enc.encounterid = fact.encounter_Num  

end
go

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

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'PCORNetVital') AND type in (N'P', N'PC'))
DROP PROCEDURE PCORNetVital
GO

create procedure PCORNetVital as
begin
-- jgk: I took out admit_date - it doesn't appear in the scheme. Now in SQLServer format - date, substring, name on inner select, no nested with. Added modifiers and now use only pathnames, not codes.
-- TODO: There is no data for the modifiers. Needs testing.
insert into pmnVITAL(patid, encounterid, measure_date, measure_time,vital_source,ht, wt, diastolic, systolic, original_bmi, bp_position) 
select patid, encounterid, measure_date, measure_time, isnull(max(vital_source),'NI') vital_source, -- jgk: not in the spec, so I took it out  admit_date,
max(ht) ht, max(wt) wt, max(diastolic) diastolic, max(systolic) systolic, 
max(original_bmi) original_bmi, isnull(max(bp_position),'NI') bp_position
from (
  select vit.patid, vit.encounterid, vit.measure_date, vit.measure_time 
    , case when vit.pcori_code like '\PCORI\VITAL\HT%' then vit.nval_num else null end ht
    , case when vit.pcori_code like '\PCORI\VITAL\WT%' then vit.nval_num else null end wt
    , case when vit.pcori_code like '\PCORI\VITAL\BP\DIASTOLIC%' then vit.nval_num else null end diastolic
    , case when vit.pcori_code like '\PCORI\VITAL\BP\SYSTOLIC%' then vit.nval_num else null end systolic
    , case when vit.pcori_code like '\PCORI\VITAL\ORIGINAL_BMI%' then vit.nval_num else null end original_bmi
    , case when vit.pcori_code like '\PCORI_MOD\BP_POSITION\%' then substring(vit.pcori_code,len(vit.pcori_code)-2,2) else null end bp_position
    , case when vit.pcori_code like '\PCORI_MOD\VITAL_SOURCE\%' then substring(vit.pcori_code,len(vit.pcori_code)-2,2) else null end vital_source
    , enc.admit_date
  from pmndemographic pd
  left join (
    select 
      obs.patient_num patid, obs.encounter_num encounterid, 
      	substring(convert(varchar,obs.start_Date,20),1,10) measure_date, 
	substring(convert(varchar,obs.start_Date,20),12,5) measure_time, 
      nval_num, codes.pcori_code
    from i2b2fact obs
    inner join (select c_basecode concept_cd, c_fullname pcori_code
      from (
        select '\PCORI\VITAL\BP\DIASTOLIC\' concept_path 
        union all
        select '\PCORI\VITAL\BP\SYSTOLIC\' concept_path 
        union all
        select '\PCORI\VITAL\HT\' concept_path
        union all
        select '\PCORI\VITAL\WT\' concept_path
        union all
        select '\PCORI\VITAL\ORIGINAL_BMI\' concept_path
        union all
        select '\PCORI_MOD\BP_POSITION\' concept_path
        union all
        select '\PCORI_MOD\VITAL_SOURCE\' concept_path
        ) bp, pcornet_vital pm
      where pm.c_fullname like bp.concept_path + '%'
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
group by patid, encounterid, measure_date, measure_time, admit_date

end 
go

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 6. Enrollment - v5 by Jeff Klann
----------------------------------------------------------------------------------------------------------------------------------------
------------------------- Enrollment Code ------------------------------------------------ 
-- 12/15/14
-- Written by Jeff Klann, PhD
-- This only suppports encounter basis right now. Should switch to algorithmic when Griffin's approach is finalized.
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'PCORNetEnroll') AND type in (N'P', N'PC'))
DROP PROCEDURE PCORNetEnroll
GO

create procedure PCORNetEnroll as
begin

INSERT INTO [pmnENROLLMENT]([PATID], [ENR_START_DATE], [ENR_END_DATE], [CHART], [BASIS]) 
    select patient_num patid, substring(convert(varchar,enr_start,20),1,10) enr_start_date
    , case when enr_end_end>enr_end then substring(convert(varchar,enr_end_end,20),1,10) else substring(convert(varchar,enr_end,20),1,10) end enr_end_date 
    ,'Y' chart, 'E' basis from 
    (select patient_num, min(start_date) enr_start,max(start_date) enr_end,max(end_date) enr_end_end from i2b2fact where patient_num in (select patid from pmndemographic) group by patient_num) x


end
go


----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 7. clear Program - includes all tables
----------------------------------------------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'pcornetclear') AND type in (N'P', N'PC')) DROP PROCEDURE pcornetclear
go

create procedure pcornetclear
as
begin


DELETE FROM pmnprocedure
DELETE FROM pmndiagnosis
DELETE FROM pmnvital
DELETE FROM pmnenrollment
DELETE FROM pmnencounter
DELETE FROM pmndemographic

end
go

----------------------------------------------------------------------------------------------------------------------------------------
-- 8. Load Program
----------------------------------------------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'pcornetloader') AND type in (N'P', N'PC')) DROP PROCEDURE pcornetloader
go

create procedure pcornetloader
as
begin

exec pcornetclear
exec PCORNetDemographics
exec PCORNetEncounter
exec PCORNetDiagnosis
exec PCORNetProcedure
exec PCORNetVital
exec PCORNetEnroll
exec pcornetreport

end
go

----------------------------------------------------------------------------------------------------------------------------------------
-- 9. Report Results - Version 5 by Aaron Abend and Jeff Klann
-- This version is useful to check against i2b2, but consider running the more detailed annotated data dictionary tool also.
----------------------------------------------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'pcornetReport') AND type in (N'P', N'PC')) DROP PROCEDURE pcornetReport
go
 
create procedure pcornetReport 
as
declare @i2b2pats  numeric
declare @i2b2Encounters numeric
declare @i2b2facts numeric
declare @i2b2dxs numeric
declare @i2b2procs numeric
declare @i2b2lcs numeric

declare @pmnpats  numeric
declare @pmnencounters numeric
declare @pmndx numeric
declare @pmnprocs numeric
declare @pmnfacts numeric
declare @pmnenroll numeric
declare @pmnvital numeric

declare @runid numeric
begin

select @i2b2Pats =count(*)  from i2b2patient
select @i2b2Encounters=count(*)   from i2b2visit i inner join pmndemographic d on i.patient_num=d.patid
--select @i2b2Facts=count(*)   from i2b2fact where concept_Cd like 'ICD9%'

select @pmnPats=count(*)   from pmndemographic
select @pmnencounters=count(*)   from pmnencounter e 
select @pmndx=count(*)   from pmndiagnosis
select @pmnprocs =count(*)  from pmnprocedure
select @pmnenroll =count(*)  from pmnenrollment
select @pmnvital =count(*)  from pmnvital

select @runid = max(runid) from i2pReport
set @runid = @runid + 1
insert into i2pReport select @runid, getdate(), 'Pats',			@i2b2pats,		@pmnpats,			@i2b2pats-@pmnpats
insert into i2pReport select @runid, getdate(), 'Enrollment',	@i2b2pats,		@pmnenroll,			@i2b2pats-@pmnpats

insert into i2pReport select @runid, getdate(), 'Encounters',	@i2b2Encounters,@pmnEncounters,		@i2b2encounters-@pmnencounters
insert into i2pReport select @runid, getdate(), 'DXPX',		null,		@pmndx+@pmnprocs,	null
insert into i2pReport select @runid, getdate(), 'Vital',		null,		@pmnvital,	null

select concept 'Data Type',sourceval 'From i2b2',destval 'In PopMedNet', diff 'Difference' from i2preport where runid=@runid

end
go