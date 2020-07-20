insert into cdm_status (task, start_time) select 'obs_clin', sysdate from dual
/
BEGIN
PMN_DROPSQL('drop table cardiolabcomponents');
END;
/
create table cardiolabcomponents as
select distinct cor.component_id,ceap.proc_name from clarity.order_results cor
left join clarity.order_proc cop on cop.order_proc_id=cor.order_proc_id
left join clarity.clarity_eap ceap on cop.proc_id=ceap.proc_id
where ceap.proc_name like '%ECHOCARDIOGRAM%'
/
BEGIN
PMN_DROPSQL('DROP SEQUENCE obs_clin_seq');
END;
/
create sequence  obs_clin_seq cache 2000
/
BEGIN
PMN_DROPSQL('DROP TABLE obs_clin');
END;
/
CREATE TABLE obs_clin(
    OBSCLINID varchar(50) NOT NULL,
    PATID varchar(50) NOT NULL,
    ENCOUNTERID varchar(50) NULL,
    OBSCLIN_PROVIDERID varchar(50) NULL,
    OBSCLIN_DATE date NULL,
    OBSCLIN_TIME varchar(5) NULL,
    OBSCLIN_TYPE varchar(2) NULL,
    OBSCLIN_CODE varchar(50) NULL,
    OBSCLIN_RESULT_QUAL varchar(50) NULL,
    OBSCLIN_RESULT_TEXT varchar(50) NULL,
    OBSCLIN_RESULT_SNOMED varchar(50) NULL,
    OBSCLIN_RESULT_NUM NUMBER(18, 0) NULL, -- (8,0)
    OBSCLIN_RESULT_MODIFIER varchar(2) NULL,
    OBSCLIN_RESULT_UNIT varchar(50) NULL,
    RAW_OBSCLIN_NAME varchar(250) NULL,
    RAW_OBSCLIN_CODE varchar(50) NULL,
    RAW_OBSCLIN_TYPE varchar(50) NULL,
    RAW_OBSCLIN_RESULT varchar(50) NULL,
    RAW_OBSCLIN_MODIFIER varchar(50) NULL,
    RAW_OBSCLIN_UNIT varchar(50) NULL,
    OBSCLIN_SOURCE varchar(5)
)
/
create table pcornet_cdm.obs_clin_all as
select distinct lab.patid
,lab.encounterid
,'  ' obsclin_providerid
,lab.lab_order_date obsclin_date
,lab.result_time obsclin_time
,lab.lab_px_type obsclin_type
,lab.lab_px obsclin_code
,lab.result_qual obsclin_result_qual
,'  ' obsclin_result_text
,'  ' obsclin_result_snomed
,lab.result_num obsclin_result_num
,lab.result_modifier obsclin_result_modifier
,lab.result_unit obsclin_result_unit
,card.proc_name raw_obsclin_name
,'  ' raw_obsclin_code
,'  ' raw_obsclin_type
,lab.raw_result raw_obsclin_result
,'  ' raw_obsclin_modifier
,'  ' raw_obsclin_unit
from pcornet_cdm.lab_result_cm lab
join pcornet_cdm.cardiolabcomponents card on substr(lab.raw_facility_code,18)=card.component_id
/
insert into obs_clin(obsclinid,patid,encounterid,obsclin_providerid,obsclin_date,obsclin_time,obsclin_type,obsclin_code,obsclin_result_qual,
                    obsclin_result_text,obsclin_result_snomed,obsclin_result_num,obsclin_result_modifier,obsclin_result_unit,raw_obsclin_name,
                    raw_obsclin_code,raw_obsclin_type,raw_obsclin_result,raw_obsclin_modifier,raw_obsclin_unit,obsclin_source)
select obs_clin_seq.nextval obsclinid
,patid
,encounterid
,obsclin_providerid
,obsclin_date
,obsclin_time
,obsclin_type
,obsclin_code
,obsclin_result_qual
,obsclin_result_text
,obsclin_result_snomed
,obsclin_result_num
,obsclin_result_modifier
,obsclin_result_unit
,raw_obsclin_name
,raw_obsclin_code
,raw_obsclin_type
,raw_obsclin_result
,raw_obsclin_modifier
,raw_obsclin_unit
,'OD' obsclin_source
from pcornet_cdm.obs_clin_all 
/
create index obs_clin_idx on obs_clin (PATID, ENCOUNTERID)
/

BEGIN
GATHER_TABLE_STATS('OBS_CLIN');
END;
/
                    
update cdm_status
set end_time = sysdate, records = (select count(*) from obs_clin)
where task = 'obs_clin'
/

select records + 1 from cdm_status where task = 'obs_clin'
