/* obs_gen - create the obs_gen table.*/
insert into cdm_status (task, start_time) select 'obs_gen', sysdate from dual
/
BEGIN
PMN_DROPSQL('drop sequence obs_gen_seq');
END;
/
create sequence  obs_gen_seq cache 2000
/
BEGIN
PMN_DROPSQL('DROP TABLE obs_gen');
END;
/
CREATE TABLE obs_gen(
    OBSGENID varchar(50) NOT NULL,
    PATID varchar(50) NOT NULL,
    ENCOUNTERID varchar(50) NULL,
    OBSGEN_PROVIDERID varchar(50) NULL,
    OBSGEN_START_DATE date NULL,
    OBSGEN_START_TIME varchar(5) NULL,
    OBSGEN_STOP_DATE date NULL,
    OBSGEN_STOP_TIME varchar(5) NULL,
    OBSGEN_TYPE varchar(30) NULL,
    OBSGEN_CODE varchar(50) NULL,
    OBSGEN_RESULT_QUAL varchar(50) NULL,
    OBSGEN_RESULT_TEXT varchar(50) NULL,
    OBSGEN_RESULT_NUM NUMBER(18, 0) NULL, -- (8,0)
    OBSGEN_RESULT_MODIFIER varchar(2) NULL,
    OBSGEN_RESULT_UNIT varchar(50) NULL,
    OBSGEN_TABLE_MODIFIED varchar(3) NULL,
    OBSGEN_ID_MODIFIED varchar(50) NULL,
    OBSGEN_ABN_IND varchar(2) NULL,
    RAW_OBSGEN_NAME varchar(50) NULL,
    RAW_OBSGEN_CODE varchar(50) NULL,
    RAW_OBSGEN_TYPE varchar(50) NULL,
    RAW_OBSGEN_RESULT varchar(50) NULL,
    RAW_OBSGEN_UNIT varchar(50) NULL,
    OBSGEN_SOURCE varchar(2) NULL
)
/

BEGIN
PMN_DROPSQL('drop table pcornet_cdm.obsgen_naaccr');
END;
/
create table pcornet_cdm.obsgen_naaccr
nologging parallel
as
select naaccrfact.patient_num, naaccrfact.encounter_num,naaccrfact.provider_id,
naaccrfact.start_date,naaccrfact.tval_char,naaccrfact.nval_num,
substr(naaccrfact.concept_cd, instr(naaccrfact.concept_cd, '|') + 1,
to_number(instr(naaccrfact.concept_cd, ':') - instr(naaccrfact.concept_cd, '|') - 1)) as code_value,
naaccrfact.concept_cd from &&i2b2_data_schema.observation_fact naaccrfact
                     join pcornet_cdm.demographic dem on naaccrfact.patient_num=dem.patid
                     where naaccrfact.concept_cd like '%NAACCR%'
                     and naaccrfact.start_date <= sysdate and naaccrfact.start_date >= date '1800-01-01' -- hospital was founded in 1906 
/

BEGIN
PMN_DROPSQL('drop index PCORNET_CDM.IX_OBSGEN_NAACCR_CD');
END;
/
BEGIN
PMN_DROPSQL('drop index PCORNET_CDM.IX_OBSGEN_NAACCR_ENC');
END;
/
BEGIN
PMN_DROPSQL('drop index PCORNET_CDM.IX_LOINC_NAACCR_CD');
END;
/

alter table obs_gen nologging
/
CREATE BITMAP INDEX PCORNET_CDM.IX_OBSGEN_NAACCR_CD ON PCORNET_CDM.OBSGEN_NAACCR (CODE_VALUE ASC) parallel nologging
/
CREATE BITMAP INDEX PCORNET_CDM.IX_OBSGEN_NAACCR_ENC ON PCORNET_CDM.OBSGEN_NAACCR (ENCOUNTER_NUM ASC) parallel nologging
/
CREATE BITMAP INDEX PCORNET_CDM.IX_LOINC_NAACCR_CD ON PCORNET_CDM.LOINC_NAACCR (CODE_VALUE ASC) parallel nologging
/

insert /*+ APPEND */  into obs_gen(obsgenid,patid,encounterid,obsgen_providerid,obsgen_start_date,obsgen_code,obsgen_result_text,
                    obsgen_result_num,obsgen_source,raw_obsgen_code)                     
select /*+ parallel(6) */
obs_gen_seq.nextval obsgenid,
obs.patient_num patid,
--case when obs.encounter_num not in (select  encounterid from pcornet_cdm.encounter) then NULL else obs.encounter_num end encounterid,
enc.encounterid,
case when obs.provider_id = '@' then NULL else obs.provider_id end obsgen_providerid,
obs.start_date obsgen_start_date,
lc.loinc_num obsgen_code,
obs.tval_char obsgen_result_text,
obs.nval_num obsgen_result_num,
'RG' obsgen_source,
obs.concept_cd raw_obsgen_code
from pcornet_cdm.obsgen_naaccr obs
left join pcornet_cdm.loinc_naaccr lc on obs.code_value =lc.code_value
left join pcornet_cdm.encounter enc on  obs.encounter_num= enc.encounterid
/
create index obs_gen_idx on obs_gen(PATID, ENCOUNTERID)
/
update cdm_status
set end_time = sysdate, records = (select count(*) from obs_gen)
where task = 'obs_gen'
/
select records + 1 from cdm_status where task = 'obs_gen'
/

