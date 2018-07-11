/** demographic - create and populate the demographic table.
*/
insert into cdm_status (task, start_time) select 'demographic', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE demographic');
END;
/
CREATE TABLE demographic(
    PATID varchar(50) NOT NULL,
    BIRTH_DATE date NULL,
    BIRTH_TIME varchar(5) NULL,
    SEX varchar(2) NULL,
    SEXUAL_ORIENTATION varchar(2) NULL,
    GENDER_IDENTITY varchar(2) NULL,
    HISPANIC varchar(2) NULL,
    BIOBANK_FLAG varchar(1) DEFAULT 'N',
    RACE varchar(2) NULL,
    PAT_PREF_LANGUAGE_SPOKEN varchar(3) NULL,
    RAW_SEX varchar(50) NULL,
    RAW_SEXUAL_ORIENTATION varchar(50) NULL,
    RAW_GENDER_IDENTITY varchar(50) NULL,
    RAW_HISPANIC varchar(50) NULL,
    RAW_RACE varchar(50) NULL
)
/
create or replace procedure PCORNetDemographic as

sqltext varchar2(4000);
cursor getsql is
--1 --  S,R,NH
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, SEXUAL_ORIENTATION, GENDER_IDENTITY, HISPANIC, RACE) '||
	'	select ''1'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH24:MI''), '||
	''''||sex.pcori_basecode||''','||
  '''NI'','||
  '''NI'','||
	'''NI'','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(p.sex_cd) in ('||lower(sex.c_dimcode)||') '||
	'    and    lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'   and lower(nvl(p.ethnicity_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo race, pcornet_demo sex
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union -- A - S,R,H
select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, SEXUAL_ORIENTATION, GENDER_IDENTITY, HISPANIC, RACE) '||
	'	select ''A'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH24:MI''), '||
	''''||sex.pcori_basecode||''','||
  '''NI'','||
  '''NI'','||
	''''||hisp.pcori_basecode||''','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(p.sex_cd) in ('||lower(sex.c_dimcode)||') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'	and	lower(p.ethnicity_cd) in ('||lower(hisp.c_dimcode)||') '
	from pcornet_demo race, pcornet_demo hisp, pcornet_demo sex
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%'
	and hisp.c_visualattributes like 'L%'
	and sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --2 S, nR, nH
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, SEXUAL_ORIENTATION, GENDER_IDENTITY, HISPANIC, RACE) '||
	'	select ''2'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH24:MI''), '||
	''''||sex.pcori_basecode||''','||
  '''NI'','||
  '''NI'','||
	'''NI'','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) in ('||lower(sex.c_dimcode)||') '||
	'	and	lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '||
	'   and lower(nvl(p.ethnicity_cd,''ni'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '
	from pcornet_demo sex
	where sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
union --3 -- nS,R, NH
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, SEXUAL_ORIENTATION, GENDER_IDENTITY, HISPANIC, RACE) '||
	'	select ''3'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH24:MI''), '||
	'''NI'','||
  '''NI'','||
  '''NI'','||
	'''NI'','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'   and lower(nvl(p.ethnicity_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'')'
	from pcornet_demo race
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
union --B -- nS,R, H
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, SEXUAL_ORIENTATION, GENDER_IDENTITY, HISPANIC, RACE) '||
	'	select ''B'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH24:MI''), '||
	'''NI'','||
  '''NI'','||
  '''NI'','||
	''''||hisp.pcori_basecode||''','||
	''''||race.pcori_basecode||''''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and	lower(p.race_cd) in ('||lower(race.c_dimcode)||') '||
	'	and	lower(p.ethnicity_cd) in ('||lower(hisp.c_dimcode)||') '
	from pcornet_demo race,pcornet_demo hisp
	where race.c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
	and race.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%'
	and hisp.c_visualattributes like 'L%'
union --4 -- S, NR, H
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, SEXUAL_ORIENTATION, GENDER_IDENTITY, HISPANIC, RACE) '||
	'	select ''4'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH24:MI''), '||
	''''||sex.pcori_basecode||''','||
  '''NI'','||
  '''NI'','||
	''''||hisp.pcori_basecode||''','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''NI'')) in ('||lower(sex.c_dimcode)||') '||
	'	and lower(nvl(p.ethnicity_cd,''NI'')) in ('||lower(hisp.c_dimcode)||') '||
	'	and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '
	from pcornet_demo sex, pcornet_demo hisp
	where sex.c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
	and sex.c_visualattributes like 'L%'
	and hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%'
	and hisp.c_visualattributes like 'L%'
union --5 -- NS, NR, H
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, SEXUAL_ORIENTATION, GENDER_IDENTITY, HISPANIC, RACE) '||
	'	select ''5'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH24:MI''), '||
	'''NI'','||
  '''NI'','||
  '''NI'','||
	''''||hisp.pcori_basecode||''','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '||
	'	and lower(nvl(p.ethnicity_cd,''NI'')) in ('||lower(hisp.c_dimcode)||') '
  from pcornet_demo hisp
	where hisp.c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%'
	and hisp.c_visualattributes like 'L%'
union --6 -- NS, NR, nH
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, SEXUAL_ORIENTATION, GENDER_IDENTITY, HISPANIC, RACE) '||
	'	select ''6'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH24:MI''), '||
	'''NI'','||
  '''NI'','||
  '''NI'','||
	'''NI'','||
	'''NI'''||
	' from i2b2patient p '||
	'	where lower(nvl(p.sex_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''SEX'') '||
	'	and lower(nvl(p.ethnicity_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''HISPANIC'') '||
	'   and lower(nvl(p.race_cd,''xx'')) not in (select lower(code) from pcornet_codelist where codetype=''RACE'') '
	from dual;

begin
pcornet_popcodelist;

PMN_DROPSQL('drop index demographic_pk');

execute immediate 'truncate table demographic';

OPEN getsql;
LOOP
FETCH getsql INTO sqltext;
	EXIT WHEN getsql%NOTFOUND;
--	insert into st values (sqltext);
	execute immediate sqltext;
	COMMIT;
END LOOP;
CLOSE getsql;

-- Logic of the using statment is -
-- When the source (i2b2patient) doesn't provide a lanaguage_cd, substitute 'no information', which will be coded
-- as 'NI'.  When the source provides a language_cd that isn't in the mapping table, substitute the code 'OT'.
merge into demographic d
using (
  select NVL(code, 'OT') as code, language_cd, patient_num
  from language_code
  right join i2b2patient on
  case when language_cd is NULL then 'no information' else language_cd end = lower(descriptive_text)
) l
on (d.patid = l.patient_num)
when matched then update
set d.PAT_PREF_LANGUAGE_SPOKEN = l.code;

execute immediate 'create unique index demographic_pk on demographic (PATID)';
GATHER_TABLE_STATS('DEMOGRAPHIC');

end PCORNetDemographic;
/
BEGIN
PCORNetDemographic();
END;
/
update cdm_status
set end_time = sysdate, records = (select count(*) from demographic)
where task = 'demographic'
/
select records from cdm_status where task = 'demographic'