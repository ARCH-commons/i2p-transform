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
--undef datamart_id;
--undef datamart_name;
--undef network_id;
--undef network_name;



create or replace procedure PCORNetDemographic as 

sqltext varchar2(4000); 
cursor getsql is 
--1 --  S,R,NH
	select 'insert into demographic(raw_sex,PATID, BIRTH_DATE, BIRTH_TIME,SEX, SEXUAL_ORIENTATION, GENDER_IDENTITY, HISPANIC, RACE) '||
	'	select ''1'',patient_num, '||
	'	birth_date, '||
	'	to_char(birth_date,''HH:MI''), '||
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
	'	to_char(birth_date,''HH:MI''), '||
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
	'	to_char(birth_date,''HH:MI''), '||
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
	'	to_char(birth_date,''HH:MI''), '||
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
	'	to_char(birth_date,''HH:MI''), '||
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
	'	to_char(birth_date,''HH:MI''), '||
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
	'	to_char(birth_date,''HH:MI''), '||
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
	'	to_char(birth_date,''HH:MI''), '||
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

execute immediate 'create unique index demographic_pk on demographic (PATID)';
GATHER_TABLE_STATS('DEMOGRAPHIC');

end PCORNetDemographic; 
/



/* TODOs: 
4)
ORA-00904: "FACILITY_ID": invalid identifier

5)
ORA-00904: "LOCATION_ZIP": invalid identifier
*/
create or replace procedure PCORNetEncounter as
begin

PMN_DROPSQL('drop index encounter_pk');
PMN_DROPSQL('drop index encounter_idx');
PMN_DROPSQL('drop index drg_idx');

execute immediate 'truncate table encounter';
execute immediate 'truncate table drg';

insert into drg
select * from
(select patient_num,encounter_num,drg_type, drg,row_number() over (partition by  patient_num, encounter_num order by drg_type desc) AS rn from
(select patient_num,encounter_num,drg_type,max(drg) drg  from
(select distinct f.patient_num,encounter_num,SUBSTR(c_fullname,22,2) drg_type,SUBSTR(pcori_basecode,INSTR(pcori_basecode, ':')+1,3) drg from i2b2fact f
inner join demographic d on f.patient_num=d.patid
inner join pcornet_enc enc on enc.c_basecode  = f.concept_cd
and enc.c_fullname like '\PCORI\ENCOUNTER\DRG\%') drg1 group by patient_num,encounter_num,drg_type) drg) drg
where rn=1;

execute immediate 'create index drg_idx on drg (patient_num, encounter_num)';
GATHER_TABLE_STATS('drg');

insert into encounter(PATID,ENCOUNTERID,admit_date ,ADMIT_TIME , 
		DISCHARGE_DATE ,DISCHARGE_TIME ,PROVIDERID ,FACILITY_LOCATION  
		,ENC_TYPE ,FACILITYID ,DISCHARGE_DISPOSITION , 
		DISCHARGE_STATUS ,DRG ,DRG_TYPE ,ADMITTING_SOURCE) 
select distinct v.patient_num, v.encounter_num,  
	start_Date, 
	to_char(start_Date,'HH:MI'), 
	end_Date, 
	to_char(end_Date,'HH:MI'), 
	providerid,
  'NI' location_zip, /* See TODO above */
(case when pcori_enctype is not null then pcori_enctype else 'UN' end) enc_type, 
  'NI' facility_id,  /* See TODO above */
  CASE WHEN pcori_enctype='AV' THEN 'NI' ELSE  discharge_disposition END, 
  CASE WHEN pcori_enctype='AV' THEN 'NI' ELSE discharge_status END, 
  drg.drg, drg_type, 
  CASE WHEN admitting_source IS NULL THEN 'NI' ELSE admitting_source END admitting_source
from i2b2visit v inner join demographic d on v.patient_num=d.patid
left outer join drg -- This section is bugfixed to only include 1 drg if multiple DRG types exist in a single encounter...
  on drg.patient_num=v.patient_num and drg.encounter_num=v.encounter_num
left outer join 
-- Encounter type. Note that this requires a full table scan on the ontology table, so it is not particularly efficient.
(select patient_num, encounter_num, inout_cd,SUBSTR(pcori_basecode,INSTR(pcori_basecode, ':')+1,2) pcori_enctype from i2b2visit v
 inner join pcornet_enc e on c_dimcode like '%'''||inout_cd||'''%' and e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%') enctype
  on enctype.patient_num=v.patient_num and enctype.encounter_num=v.encounter_num;

execute immediate 'create unique index encounter_pk on encounter (ENCOUNTERID)';
execute immediate 'create index encounter_idx on encounter (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('ENCOUNTER');

end PCORNetEncounter;
/



create or replace procedure PCORNetDiagnosis as
begin

PMN_DROPSQL('drop index diagnosis_idx');
PMN_DROPSQL('drop index sourcefact_idx');
PMN_DROPSQL('drop index pdxfact_idx');
PMN_DROPSQL('drop index originfact_idx');

execute immediate 'truncate table diagnosis';
execute immediate 'truncate table sourcefact';
execute immediate 'truncate table pdxfact';
execute immediate 'truncate table originfact';

insert into sourcefact
	select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode dxsource, dxsource.c_fullname
	from i2b2fact factline
    inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num
    inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode
	where dxsource.c_fullname like '\PCORI_MOD\CONDITION_OR_DX\%';

execute immediate 'create index sourcefact_idx on sourcefact (patient_num, encounter_num, provider_id, concept_cd, start_date)';
GATHER_TABLE_STATS('SOURCEFACT');

insert into pdxfact
	select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode pdxsource,dxsource.c_fullname
	from i2b2fact factline
    inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num
    inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode
	and dxsource.c_fullname like '\PCORI_MOD\PDX\%';

execute immediate 'create index pdxfact_idx on pdxfact (patient_num, encounter_num, provider_id, concept_cd, start_date)';
GATHER_TABLE_STATS('PDXFACT');

insert into originfact --CDM 3.1 addition
	select patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode originsource, dxsource.c_fullname  
	from i2b2fact factline 
    inner join ENCOUNTER enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num 
    inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode 
	and dxsource.c_fullname like '\PCORI_MOD\DX_ORIGIN\%';

execute immediate 'create index originfact_idx on originfact (patient_num, encounter_num, provider_id, concept_cd, start_date)';
GATHER_TABLE_STATS('ORIGINFACT');

insert into diagnosis (patid,	encounterid, enc_type, admit_date, providerid, dx, dx_type, dx_source, dx_origin, pdx)
/* KUMC started billing with ICD10 on Oct 1, 2015. */
with icd10_transition as (
  select date '2015-10-01' as cutoff from dual
)

/* Encoding of DX type from the CDM 3.1 Specification
   http://pcornet.org/wp-content/uploads/2016/11/2016-11-15-PCORnet-Common-Data-Model-v3.1_Specification.pdf
 */
, dx_type as (
  select '09' as icd_9_cm
       , '10' as icd_10_cm
       , '11' as icd_11_cm
       , 'SM' as snomed_ct
       , 'NI' as no_info
       , 'UN' as unknown
       , 'OT' as other
  from dual)
  
/* DX_IDs may have mappings to both ICD9 and ICD10 */
, has9 as (
  select distinct c_basecode, pcori_basecode icd9_code
  from "&&i2b2_meta_schema".pcornet_diag diag
  where diag.c_fullname like '\PCORI\DIAGNOSIS\09%'
  and pcori_basecode is not null
)
, has10 as (
  select distinct c_basecode, pcori_basecode icd10_code
  from "&&i2b2_meta_schema".pcornet_diag diag
  where diag.c_fullname like '\PCORI\DIAGNOSIS\10%'
  and pcori_basecode is not null
)
, diag as (
  select distinct diag.c_basecode, diag.pcori_basecode, has9.icd9_code, has10.icd10_code
       , case when diag.pcori_basecode = has9.icd9_code then (select icd_9_cm from dx_type) 
              when diag.pcori_basecode = has10.icd10_code then (select icd_10_cm from dx_type)
              else (select no_info from dx_type)
         end dx_type
  from pcornet_diag diag
  left join has9 on has9.c_basecode = diag.c_basecode
  left join has10 on has10.c_basecode = diag.c_basecode

  where diag.c_fullname like '\PCORI\DIAGNOSIS\%'
  and diag.pcori_basecode is not null
)
/* Convert i2b2 diagnosis facts to pcori diagnosis facts
 * Note: The mapping of i2b2 diagnosis facts to pcori diagnosis facts 
 * with respect to ICD9 and ICD10 is not 1-to-1 (there may exist an i2b2 
 * diagnosis fact that could be represented as both ICD9 and ICD10 pcori fact)
 *
 * To prevent an increase in the number of post conversion facts, prioritize 
 * filtering ICD10s prior to 1/1/2015 and ICD9s after 1/1/2015 if resulting 
 * facts produce duplicates based on the i2b2 fact primary key.
 */
, diag_fact_merge as (
 select factline.*, diag.*
      , row_number() over (partition by factline.patient_num
                                      , factline.encounter_num
                                      , factline.concept_cd
                                      , factline.start_date
                                      , factline.modifier_cd
                                      , factline.instance_num 
                          order by (case when diag.dx_type = (select icd_9_cm from dx_type)  
                                          and factline.start_date >= (select cutoff from icd10_transition) then 0
                                         when diag.dx_type = (select icd_10_cm from dx_type)
                                          and factline.start_date < (select cutoff from icd10_transition) then 0
                                         else 1
                                    end) asc) unique_row
 from i2b2fact factline
 join diag on diag.c_basecode = factline.concept_cd
)
, diag_fact_cutoff_filter as (
 select * from diag_fact_merge where unique_row = 1
)

select distinct factline.patient_num, factline.encounter_num encounterid,	enc_type, enc.admit_date, enc.providerid
     , factline.pcori_basecode dx
     , factline.dx_type dxtype,
	CASE WHEN enc_type='AV' THEN 'FI' ELSE nvl(SUBSTR(dxsource,INSTR(dxsource,':')+1,2) ,'NI') END dx_source,
  'BI' dx_origin,
	CASE WHEN enc_type in ('EI', 'IP', 'IS')  -- PDX is "relevant only on IP and IS encounters"
             THEN nvl(SUBSTR(pdxsource,INSTR(pdxsource, ':')+1,2),'NI')
             ELSE 'X' END PDX
from diag_fact_cutoff_filter factline
inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num
 left outer join sourcefact
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
and factline.start_date=pdxfact.start_Date
left outer join originfact --CDM 3.1 addition
on	factline.patient_num=originfact.patient_num 
and factline.encounter_num=originfact.encounter_num
and factline.provider_id=originfact.provider_id 
and factline.concept_cd=originfact.concept_cd
and factline.start_date=originfact.start_Date
where (sourcefact.c_fullname like '\PCORI_MOD\CONDITION_OR_DX\DX_SOURCE\%' or sourcefact.c_fullname is null)
-- order by enc.admit_date desc
;

execute immediate 'create index diagnosis_idx on diagnosis (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('DIAGNOSIS');

end PCORNetDiagnosis;
/



create or replace procedure PCORNetCondition as
begin

PMN_DROPSQL('drop index condition_idx');
PMN_DROPSQL('drop index sourcefact2_idx');

execute immediate 'truncate table condition';
execute immediate 'truncate table sourcefact2';

insert into sourcefact2
	select distinct patient_num, encounter_num, provider_id, concept_cd, start_date, dxsource.pcori_basecode dxsource, dxsource.c_fullname
	from i2b2fact factline
    inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num
    inner join pcornet_diag dxsource on factline.modifier_cd =dxsource.c_basecode
	where dxsource.c_fullname like '\PCORI_MOD\CONDITION_OR_DX\%';

execute immediate 'create index sourcefact2_idx on sourcefact2 (patient_num, encounter_num, provider_id, concept_cd, start_date)';
GATHER_TABLE_STATS('SOURCEFACT2');

create_error_table('CONDITION');

insert into condition (patid, encounterid, report_date, resolve_date, condition, condition_type, condition_status, condition_source)
select distinct factline.patient_num, min(factline.encounter_num) encounterid, min(factline.start_date) report_date, NVL(max(factline.end_date),null) resolve_date, diag.pcori_basecode,
SUBSTR(diag.c_fullname,18,2) condition_type,
	NVL2(max(factline.end_date) , 'RS', 'NI') condition_status, -- Imputed so might not be entirely accurate
	NVL(SUBSTR(max(dxsource),INSTR(max(dxsource), ':')+1,2),'NI') condition_source
from i2b2fact factline
inner join encounter enc on enc.patid = factline.patient_num and enc.encounterid = factline.encounter_Num
inner join pcornet_diag diag on diag.c_basecode  = factline.concept_cd
 left outer join sourcefact2 sf
on	factline.patient_num=sf.patient_num
and factline.encounter_num=sf.encounter_num
and factline.provider_id=sf.provider_id
and factline.concept_cd=sf.concept_Cd
and factline.start_date=sf.start_Date
where diag.c_fullname like '\PCORI\DIAGNOSIS\%'
and sf.c_fullname like '\PCORI_MOD\CONDITION_OR_DX\CONDITION_SOURCE\%'
group by factline.patient_num, diag.pcori_basecode, diag.c_fullname
log errors into ERR$_CONDITION reject limit unlimited
;

execute immediate 'create index condition_idx on condition (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('CONDITION');

end PCORNetCondition;
/



create or replace procedure PCORNetProcedure as
begin

PMN_DROPSQL('drop index procedures_idx');

execute immediate 'truncate table procedures';

insert into procedures( 
				patid,			encounterid,	enc_type, admit_date, px_date, providerid, px, px_type, px_source) 
select  distinct fact.patient_num, enc.encounterid,	enc.enc_type, enc.admit_date, fact.start_date, 
		fact.provider_id, SUBSTR(pr.pcori_basecode,INSTR(pr.pcori_basecode, ':')+1,11) px, SUBSTR(pr.c_fullname,18,2) pxtype,
    -- All are billing for now - see https://informatics.gpcnetwork.org/trac/Project/ticket/491
    'BI' px_source
from i2b2fact fact
 inner join	pcornet_proc pr on pr.c_basecode  = fact.concept_cd
 inner join encounter enc on enc.patid = fact.patient_num and enc.encounterid = fact.encounter_Num
where pr.c_fullname like '\PCORI\PROCEDURE\%';

execute immediate 'create index procedures_idx on procedures (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('PROCEDURES');

end PCORNetProcedure;
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






create or replace procedure PCORNetEnroll as
begin

PMN_DROPSQL('drop index enrollment_idx');

execute immediate 'truncate table enrollment';

INSERT INTO enrollment(PATID, ENR_START_DATE, ENR_END_DATE, CHART, ENR_BASIS) 
with pats_delta as (
  -- If only one visit, visit_delta_days will be 0
  select patient_num, max(start_date) - min(start_date) visit_delta_days
  from i2b2visit
  where start_date > add_months(sysdate, -&&enrollment_months_back)
  group by patient_num
  ),
enrolled as (
  select distinct patient_num 
  from pats_delta
  where visit_delta_days > 30
  )
select 
  visit.patient_num patid, min(visit.start_date) enr_start_date, 
  max(visit.start_date) enr_end_date, 'Y' chart, 'A' enr_basis
from enrolled enr
join i2b2visit visit on enr.patient_num = visit.patient_num
group by visit.patient_num;

execute immediate 'create index enrollment_idx on enrollment (PATID)';
GATHER_TABLE_STATS('ENROLLMENT');

end PCORNetEnroll;
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
to_char(m.start_date,'HH:MI')  SPECIMEN_TIME,
m.end_date RESULT_DATE,
to_char(m.end_date,'HH:MI') RESULT_TIME,
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
nullif(norm.NORM_RANGE_LOW,'') NORM_RANGE_LOW
,norm.NORM_MODIFIER_LOW,
nullif(norm.NORM_RANGE_HIGH,'') NORM_RANGE_HIGH
,norm.NORM_MODIFIER_HIGH,
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

inner join pcornet_lab lab on lab.c_basecode  = M.concept_cd and lab.c_fullname like '\PCORI\LAB_RESULT_CM\%'
inner JOIN pcornet_lab ont_parent on lab.c_path=ont_parent.c_fullname
left outer join pmn_labnormal norm on ont_parent.c_basecode=norm.LAB_NAME

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
and m.MODIFIER_CD='@';

execute immediate 'create index lab_result_cm_idx on lab_result_cm (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('LAB_RESULT_CM');

END PCORNetLabResultCM;
/




create or replace procedure PCORNetHarvest as
begin

execute immediate 'truncate table harvest';

INSERT INTO harvest(NETWORKID, NETWORK_NAME, DATAMARTID, DATAMART_NAME, DATAMART_PLATFORM, CDM_VERSION, DATAMART_CLAIMS, DATAMART_EHR, BIRTH_DATE_MGMT, ENR_START_DATE_MGMT, ENR_END_DATE_MGMT, ADMIT_DATE_MGMT, DISCHARGE_DATE_MGMT, PX_DATE_MGMT, RX_ORDER_DATE_MGMT, RX_START_DATE_MGMT, RX_END_DATE_MGMT, DISPENSE_DATE_MGMT, LAB_ORDER_DATE_MGMT, SPECIMEN_DATE_MGMT, RESULT_DATE_MGMT, MEASURE_DATE_MGMT, ONSET_DATE_MGMT, REPORT_DATE_MGMT, RESOLVE_DATE_MGMT, PRO_DATE_MGMT, REFRESH_DEMOGRAPHIC_DATE, REFRESH_ENROLLMENT_DATE, REFRESH_ENCOUNTER_DATE, REFRESH_DIAGNOSIS_DATE, REFRESH_PROCEDURES_DATE, REFRESH_VITAL_DATE, REFRESH_DISPENSING_DATE, REFRESH_LAB_RESULT_CM_DATE, REFRESH_CONDITION_DATE, REFRESH_PRO_CM_DATE, REFRESH_PRESCRIBING_DATE, REFRESH_PCORNET_TRIAL_DATE, REFRESH_DEATH_DATE, REFRESH_DEATH_CAUSE_DATE) 
	select '&&network_id', '&&network_name', getDataMartID(), getDataMartName(), getDataMartPlatform(), 3, hl.DATAMART_CLAIMS, hl.DATAMART_EHR, hl.BIRTH_DATE_MGMT, hl.ENR_START_DATE_MGMT, hl.ENR_END_DATE_MGMT, hl.ADMIT_DATE_MGMT, hl.DISCHARGE_DATE_MGMT, hl.PX_DATE_MGMT, hl.RX_ORDER_DATE_MGMT, hl.RX_START_DATE_MGMT, hl.RX_END_DATE_MGMT, hl.DISPENSE_DATE_MGMT, hl.LAB_ORDER_DATE_MGMT, hl.SPECIMEN_DATE_MGMT, hl.RESULT_DATE_MGMT, hl.MEASURE_DATE_MGMT, hl.ONSET_DATE_MGMT, hl.REPORT_DATE_MGMT, hl.RESOLVE_DATE_MGMT, hl.PRO_DATE_MGMT,
  case when (select count(*) from demographic) > 0 then current_date else null end REFRESH_DEMOGRAPHIC_DATE,
  case when (select count(*) from enrollment) > 0 then current_date else null end REFRESH_ENROLLMENT_DATE,
  case when (select count(*) from encounter) > 0 then current_date else null end REFRESH_ENCOUNTER_DATE,
  case when (select count(*) from diagnosis) > 0 then current_date else null end REFRESH_DIAGNOSIS_DATE,
  case when (select count(*) from procedures) > 0 then current_date else null end REFRESH_PROCEDURES_DATE,
  case when (select count(*) from vital) > 0 then current_date else null end REFRESH_VITAL_DATE,
  case when (select count(*) from dispensing) > 0 then current_date else null end REFRESH_DISPENSING_DATE,
  case when (select count(*) from lab_result_cm) > 0 then current_date else null end REFRESH_LAB_RESULT_CM_DATE,
  case when (select count(*) from condition) > 0 then current_date else null end REFRESH_CONDITION_DATE,
  case when (select count(*) from pro_cm) > 0 then current_date else null end REFRESH_PRO_CM_DATE,
  case when (select count(*) from prescribing) > 0 then current_date else null end REFRESH_PRESCRIBING_DATE,
  case when (select count(*) from pcornet_trial) > 0 then current_date else null end REFRESH_PCORNET_TRIAL_DATE,
  case when (select count(*) from death) > 0 then current_date else null end REFRESH_DEATH_DATE,
  case when (select count(*) from death_cause) > 0 then current_date else null end REFRESH_DEATH_CAUSE_DATE
  from harvest_local hl;

end PCORNetHarvest;
/




create or replace procedure PCORNetPrescribing as
begin

PMN_DROPSQL('drop index prescribing_idx');
PMN_DROPSQL('drop index basis_idx');
PMN_DROPSQL('drop index freq_idx');
PMN_DROPSQL('drop index quantity_idx');
PMN_DROPSQL('drop index refills_idx');
PMN_DROPSQL('drop index supply_idx');

execute immediate 'truncate table prescribing';
execute immediate 'truncate table basis';
execute immediate 'truncate table freq';
execute immediate 'truncate table quantity';
execute immediate 'truncate table refills';
execute immediate 'truncate table supply';

insert into basis
select pcori_basecode,c_fullname,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact basis
        inner join encounter enc on enc.patid = basis.patient_num and enc.encounterid = basis.encounter_Num
     join pcornet_med basiscode
        on basis.modifier_cd = basiscode.c_basecode
        and basiscode.c_fullname like '\PCORI_MOD\RX_BASIS\%';

execute immediate 'create unique index basis_idx on basis (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('BASIS');

insert into freq
select pcori_basecode,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact freq
        inner join encounter enc on enc.patid = freq.patient_num and enc.encounterid = freq.encounter_Num
     join pcornet_med freqcode
        on freq.modifier_cd = freqcode.c_basecode
        and freqcode.c_fullname like '\PCORI_MOD\RX_FREQUENCY\%';

execute immediate 'create unique index freq_idx on freq (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('FREQ');

insert into quantity
select nval_num,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact quantity
        inner join encounter enc on enc.patid = quantity.patient_num and enc.encounterid = quantity.encounter_Num
     join pcornet_med quantitycode
        on quantity.modifier_cd = quantitycode.c_basecode
        and quantitycode.c_fullname like '\PCORI_MOD\RX_QUANTITY\';

execute immediate 'create unique index quantity_idx on quantity (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('QUANTITY');
        
insert into refills
select nval_num,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact refills
        inner join encounter enc on enc.patid = refills.patient_num and enc.encounterid = refills.encounter_Num
     join pcornet_med refillscode
        on refills.modifier_cd = refillscode.c_basecode
        and refillscode.c_fullname like '\PCORI_MOD\RX_REFILLS\';

execute immediate 'create unique index refills_idx on refills (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('REFILLS');
        
insert into supply
select nval_num,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact supply
        inner join encounter enc on enc.patid = supply.patient_num and enc.encounterid = supply.encounter_Num
     join pcornet_med supplycode
        on supply.modifier_cd = supplycode.c_basecode
        and supplycode.c_fullname like '\PCORI_MOD\RX_DAYS_SUPPLY\';

execute immediate 'create unique index supply_idx on supply (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('SUPPLY');

-- insert data with outer joins to ensure all records are included even if some data elements are missing
insert into prescribing (
	PATID
    ,encounterid
    ,RX_PROVIDERID
	,RX_ORDER_DATE -- using start_date from i2b2
	,RX_ORDER_TIME  -- using time start_date from i2b2
	,RX_START_DATE
	,RX_END_DATE 
    ,RXNORM_CUI --using pcornet_med pcori_cui - new column!
    ,RX_QUANTITY ---- modifier nval_num
    ,RX_QUANTITY_UNIT
    ,RX_REFILLS  -- modifier nval_num
    ,RX_DAYS_SUPPLY -- modifier nval_num
    ,RX_FREQUENCY --modifier with basecode lookup
    ,RX_BASIS --modifier with basecode lookup
    ,RAW_RX_MED_NAME
--    ,RAW_RX_FREQUENCY,
    ,RAW_RXNORM_CUI
)
select distinct  m.patient_num, m.Encounter_Num,m.provider_id,  m.start_date order_date,  to_char(m.start_date,'HH:MI'), m.start_date start_date, m.end_date, mo.pcori_cui
    ,quantity.nval_num quantity, 'NI' rx_quantity_unit, refills.nval_num refills, supply.nval_num supply, substr(freq.pcori_basecode, instr(freq.pcori_basecode, ':') + 1, 2) frequency, 
    substr(basis.pcori_basecode, instr(basis.pcori_basecode, ':') + 1, 2) basis
    , substr(mo.c_name, 1, 50) raw_rx_med_name, substr(mo.c_basecode, 1, 50) raw_rxnorm_cui
 from i2b2medfact m inner join pcornet_med mo on m.concept_cd = mo.c_basecode 
inner join encounter enc on enc.encounterid = m.encounter_Num
-- TODO: This join adds several minutes to the load - must be debugged

    left join basis
    on m.encounter_num = basis.encounter_num
    and m.concept_cd = basis.concept_Cd
    and m.start_date = basis.start_date
    and m.provider_id = basis.provider_id
    and m.instance_num = basis.instance_num

    left join  freq
    on m.encounter_num = freq.encounter_num
    and m.concept_cd = freq.concept_Cd
    and m.start_date = freq.start_date
    and m.provider_id = freq.provider_id
    and m.instance_num = freq.instance_num

    left join quantity 
    on m.encounter_num = quantity.encounter_num
    and m.concept_cd = quantity.concept_Cd
    and m.start_date = quantity.start_date
    and m.provider_id = quantity.provider_id
    and m.instance_num = quantity.instance_num

    left join refills
    on m.encounter_num = refills.encounter_num
    and m.concept_cd = refills.concept_Cd
    and m.start_date = refills.start_date
    and m.provider_id = refills.provider_id
    and m.instance_num = refills.instance_num

    left join supply
    on m.encounter_num = supply.encounter_num
    and m.concept_cd = supply.concept_Cd
    and m.start_date = supply.start_date
    and m.provider_id = supply.provider_id
    and m.instance_num = supply.instance_num

where (basis.c_fullname is null or basis.c_fullname like '\PCORI_MOD\RX_BASIS\PR\%');

execute immediate 'create index prescribing_idx on prescribing (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('PRESCRIBING');

end PCORNetPrescribing;
/




create or replace procedure PCORNetDispensing as
begin

PMN_DROPSQL('drop index dispensing_idx');

execute immediate 'truncate table dispensing';
/*
PMN_DROPSQL('DROP TABLE supply');
sqltext := 'create table supply as '||
'(select nval_num,encounter_num,concept_cd from i2b2fact supply '||
'        inner join encounter enc on enc.patid = supply.patient_num and enc.encounterid = supply.encounter_Num '||
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
*/
    
/* NOTE: New transformation developed by KUMC */

insert into dispensing (
	PATID
  ,PRESCRIBINGID
  ,DISPENSE_DATE -- using start_date from i2b2
  ,NDC --using pcornet_med pcori_ndc - new column!
  ,DISPENSE_SUP ---- modifier nval_num
  ,DISPENSE_AMT  -- modifier nval_num
--    ,RAW_NDC
)
/* Below is the Cycle 2 fix for populating the DISPENSING table  */
with disp_status as (
  select ibf.patient_num, ibf.encounter_num, ibf.concept_cd, ibf.instance_num, ibf.start_date, ibf.modifier_cd
  from i2b2fact ibf
  join "&&i2b2_meta_schema".pcornet_med pnm
    on ibf.modifier_cd=pnm.c_basecode
  where pnm.c_fullname like '\PCORI_MOD\RX_BASIS\DI\%'
    /* TODO: Generalize for other sites.  The '< 12' makes sure only 11 digit 
             codes are included. */
    and length(replace(ibf.concept_cd, 'NDC:', '')) < 12
)
, disp_quantity as (
  select ibf.patient_num, ibf.encounter_num, ibf.concept_cd, ibf.instance_num, ibf.start_date, ibf.modifier_cd, ibf.nval_num
  from i2b2fact ibf
  join "&&i2b2_meta_schema".pcornet_med pnm
    on ibf.modifier_cd=pnm.c_basecode
  where pnm.c_fullname like '\PCORI_MOD\RX_QUANTITY\%'
)
, disp_supply as (
  select ibf.patient_num, ibf.encounter_num, ibf.concept_cd, ibf.instance_num, ibf.start_date, ibf.modifier_cd, ibf.nval_num
  from i2b2fact ibf
  join "&&i2b2_meta_schema".pcornet_med pnm
    on ibf.modifier_cd=pnm.c_basecode
  where pnm.c_fullname like '\PCORI_MOD\RX_DAYS_SUPPLY\%'
)
select distinct
  st.patient_num patid,
  null prescribingid,
  st.start_date dispense_date,
  replace(st.concept_cd, 'NDC:', '') ndc, -- TODO: Generalize this for other sites.
  ds.nval_num dispense_sup,
  qt.nval_num dispense_amt
from disp_status st
left outer join disp_quantity qt
  on st.patient_num=qt.patient_num
  and st.encounter_num=qt.encounter_num
  and st.concept_cd=qt.concept_cd
  and st.instance_num=qt.instance_num
  and st.start_date=qt.start_date
left outer join disp_supply ds
  on st.patient_num=ds.patient_num
  and st.encounter_num=ds.encounter_num
  and st.concept_cd=ds.concept_cd
  and st.instance_num=ds.instance_num
  and st.start_date=ds.start_date
;

/* NOTE: The original SCILHS transformation is below.

-- insert data with outer joins to ensure all records are included even if some data elements are missing

select  m.patient_num, null,m.start_date, NVL(mo.pcori_ndc,'NA')
    ,max(supply.nval_num) sup, max(amount.nval_num) amt 
from i2b2fact m inner join pcornet_med mo
on m.concept_cd = mo.c_basecode
inner join encounter enc on enc.encounterid = m.encounter_Num

    -- jgk bugfix 11/2 - we weren't filtering dispensing events
    inner join (select pcori_basecode,c_fullname,encounter_num,concept_cd from i2b2fact basis
        inner join encounter enc on enc.patid = basis.patient_num and enc.encounterid = basis.encounter_Num
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
*/

execute immediate 'create index dispensing_idx on dispensing (PATID)';
GATHER_TABLE_STATS('DISPENSING');

end PCORNetDispensing;
/


----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- 11. Death - by Jeff Klann, PhD
-- Simple transform only pulls death date from patient dimension, does not rely on an ontology
-- Translated from MSSQL script by Matthew Joss
----------------------------------------------------------------------------------------------------------------------------------------



create or replace procedure PCORNetDeath as
begin
  
execute immediate 'truncate table death';

insert into death( patid, death_date, death_date_impute, death_source, death_match_confidence) 
select distinct pat.patient_num, pat.death_date,
case when vital_status_cd like 'X%' then 'B'
  when vital_status_cd like 'M%' then 'D'
  when vital_status_cd like 'Y%' then 'N'
  else 'OT'
  end death_date_impute,
  'NI' death_source,
  'NI' death_match_confidence
from (
	/* KUMC specific fix to address unknown death dates */
  select
    ibp.patient_num,
    case when ibf.concept_cd is not null then DATE '2100-12-31' -- in accordance with the CDM v3 spec
      else ibp.death_date end death_date,
    case when ibf.concept_cd is not null then 'OT'
      else upper(ibp.vital_status_cd) end vital_status_cd
  from i2b2patient ibp
  left join i2b2fact ibf
    on ibp.patient_num=ibf.patient_num
    and ibf.CONCEPT_CD='DEM|VITAL:yu'
) pat
where (pat.death_date is not null or vital_status_cd like 'Z%') and pat.patient_num in (select patid from demographic);

end;
/


--------------------------------------------------------------------------------
-- PCORNetPostProc procedure
--
-- Ideally this procedure would be empty, but as a matter of practice there are
-- often post processing cleanup tasks that needs to be complete.  Such tasks
-- should reside here with the goal of incorporating them into to the proper
-- procedures above.
--------------------------------------------------------------------------------
create or replace procedure PCORNetPostProc as
begin
  
  /* Copy providerid from encounter table to diagnosis, procedures tables.
  CDM specification says:
    "Please note: This is a field replicated from the ENCOUNTER table."
  */
  merge into diagnosis d
  using encounter e
     on (d.encounterid = e.encounterid)
  when matched then update set d.providerid = e.providerid;
  
  merge into procedures p
  using encounter e
     on (p.encounterid = e.encounterid)
  when matched then update set p.providerid = e.providerid;
  
  merge into prescribing p
  using encounter e
     on (p.encounterid = e.encounterid)
  when matched then update set p.rx_providerid = e.providerid;
  
  /* Currently in HERON, we have hight in cm and weight in oz (from visit vitals).
  The CDM wants height in inches and weight in pounds. */
  update vital v set v.ht = v.ht / 2.54;
  update vital v set v.wt = v.wt / 16;
  
  /* Remove rows from the PRESCRIBING table where RX_* fields are null
     TODO: Remove this when fixed in HERON
   */
  delete
  from prescribing
  where rx_basis is null
    and rx_quantity is null
    and rx_frequency is null
    and rx_refills is null
  ;
  
end PCORNetPostProc;
/


--------------------------------------------------------------------------------
-- PCORNetLoader procedure
--
-- This procedure orchestrates the execution of the procedures defined above, 
-- and consists of 13 steps.  Using the start_with_step parameter a step number
-- can be provided to start at any point in the sequence.  This is helpful when
-- an issue is encountered during execution and restart from the beginning is 
-- undesirable.
--
-- Steps:
-- 1 - PCORNetDemographic
-- 2 - PCORNetEncounter
-- 3 - PCORNetDiagnosis
-- 4 - PCORNetCondition
-- 5 - PCORNetProcedure
-- 6 - PCORNetVital
-- 7 - PCORNetEnroll
-- 8 - PCORNetLabResultCM
-- 9 - PCORNetPrescribing
-- 10 - PCORNetDispensing
-- 11 - PCORNetDeath
-- 12 - PCORNetHarvest
-- 13 - PCORNetPostProc
--
--------------------------------------------------------------------------------
create or replace PROCEDURE PCORNetLoader(start_with VARCHAR2, 
  release_name VARCHAR2, build_num NUMBER, data_source VARCHAR2
) AS
start_with_step int;
begin
  
  select step into start_with_step from (
    select 1 step, 'PCORNetDemographic' proc from dual union
    select 2 step, 'PCORNetEncounter' proc from dual union
    select 3 step, 'PCORNetDiagnosis' proc from dual union
    select 4 step, 'PCORNetCondition' proc from dual union
    select 5 step, 'PCORNetProcedure' proc from dual union
    select 6 step, 'PCORNetVital' proc from dual union
    select 7 step, 'PCORNetEnroll' proc from dual union
    select 8 step, 'PCORNetLabResultCM' proc from dual union
    select 9 step, 'PCORNetPrescribing' proc from dual union
    select 10 step, 'PCORNetDispensing' proc from dual union
    select 11 step, 'PCORNetDeath' proc from dual union
    select 12 step, 'PCORNetHarvest' proc from dual union
    select 13 step, 'PCORNetPostProc' proc from dual
  ) where proc=start_with;

  if start_with_step = 1 then
    LogTaskStart(release_name, 'PCORNetDemographic', build_num, data_source);
    PCORNetDemographic;
    LogTaskComplete(release_name, 'PCORNetDemographic', build_num, 'DEMOGRAPHIC');
  end if;
  
  if start_with_step <= 2 then
    LogTaskStart(release_name, 'PCORNetEncounter', build_num, data_source);
    PCORNetEncounter;
    LogTaskComplete(release_name, 'PCORNetEncounter', build_num, 'ENCOUNTER');
  end if;
  
  if start_with_step <= 3 then 
    LogTaskStart(release_name, 'PCORNetDiagnosis', build_num, data_source);
    PCORNetDiagnosis;
    LogTaskComplete(release_name, 'PCORNetDiagnosis', build_num, 'DIAGNOSIS');
  end if;
  
  if start_with_step <= 4 then
    LogTaskStart(release_name, 'PCORNetCondition', build_num, data_source);
    PCORNetCondition;
    LogTaskComplete(release_name, 'PCORNetCondition', build_num, 'CONDITION');
  end if;
  
  if start_with_step <= 5 then
    LogTaskStart(release_name, 'PCORNetProcedure', build_num, data_source);
    PCORNetProcedure;
    LogTaskComplete(release_name, 'PCORNetProcedure', build_num, 'PROCEDURES');
  end if;
  
  if start_with_step <= 6 then
    LogTaskStart(release_name, 'PCORNetVital', build_num, data_source);
    PCORNetVital;
    LogTaskComplete(release_name, 'PCORNetVital', build_num, 'VITAL');
  end if;
  
  if start_with_step <= 7 then
    LogTaskStart(release_name, 'PCORNetEnroll', build_num, data_source);
    PCORNetEnroll;
    LogTaskComplete(release_name, 'PCORNetEnroll', build_num, 'ENROLLMENT');
  end if;
  
  if start_with_step <= 8 then
    LogTaskStart(release_name, 'PCORNetLabResultCM', build_num, data_source);
    PCORNetLabResultCM;
    LogTaskComplete(release_name, 'PCORNetLabResultCM', build_num, 'LAB_RESULT_CM');
  end if;
  
  if start_with_step <= 9 then
    LogTaskStart(release_name, 'PCORNetPrescribing', build_num, data_source);
    PCORNetPrescribing;
    LogTaskComplete(release_name, 'PCORNetPrescribing', build_num, 'PRESCRIBING');
  end if;
  
  if start_with_step <= 10 then
    LogTaskStart(release_name, 'PCORNetDispensing', build_num, data_source);
    PCORNetDispensing;
    LogTaskComplete(release_name, 'PCORNetDispensing', build_num, 'DISPENSING');
  end if;
  
  if start_with_step <= 11 then
    LogTaskStart(release_name, 'PCORNetDeath', build_num, data_source);
    PCORNetDeath;
    LogTaskComplete(release_name, 'PCORNetDeath', build_num, 'DEATH');
  end if;
  
  if start_with_step <= 12 then
    LogTaskStart(release_name, 'PCORNetHarvest', build_num, data_source);
    PCORNetHarvest;
    LogTaskComplete(release_name, 'PCORNetHarvest', build_num, 'HARVEST');
  end if;
  
  PCORNetPostProc;
end PCORNetLoader;
/