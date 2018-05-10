/** diagnosis - create and populate the diagnosis table.
*/
insert into cdm_status (task, start_time) select 'diagnosis', sysdate from dual
/
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
    DX_ORIGIN varchar(2) NULL,
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

BEGIN
PMN_DROPSQL('DROP TABLE originfact');
END;
/

CREATE TABLE ORIGINFACT  (
	PATIENT_NUM  	NUMBER(38) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	START_DATE   	DATE NOT NULL,
	ORIGINSOURCE    VARCHAR2(50) NULL,
	C_FULLNAME   	VARCHAR2(700) NOT NULL
	)
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
    nvl(SUBSTR(originsource,INSTR(originsource, ':')+1,2),'NI') dx_origin,
	CASE WHEN enc_type in ('EI', 'IP', 'IS', 'OS')
             THEN nvl(SUBSTR(pdxsource,INSTR(pdxsource, ':')+1,2),'NI')
             ELSE null END PDX
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
BEGIN
PCORNetDiagnosis();
END;
/
update cdm_status
set end_time = sysdate, records = (select count(*) from diagnosis)
where task = 'diagnosis'
/
select 1 from cdm_status where task = 'diagnosis'
--SELECT count(DIAGNOSISID) from diagnosis where rownum = 1