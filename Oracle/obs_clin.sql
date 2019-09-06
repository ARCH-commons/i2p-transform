insert into cdm_status (task, start_time) select 'obs_clin', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE pcornet_cdm.obs_clin');
END;
/
BEGIN
PMN_DROPSQL('pcornet_cdm.cardiolabcomponents');
END;

create table pcornet_cdm.cardiolabcomponents as
select distinct cor.component_id,ceap.proc_name from clarity.order_results cor
left join clarity.order_proc cop on cop.order_proc_id=cor.order_proc_id
left join clarity.clarity_eap ceap on cop.proc_id=ceap.proc_id
where ceap.proc_name like '%ECHOCARDIOGRAM%';

create sequence  obs_clin_seq cache 2000;

create table pcornet_cdm.obs_clin as
select obs_clin_seq.nextval obsclinid
, lab.patid
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
left join pcornet_cdm.cardiolabcomponents card on substr(lab.raw_facility_code,18)=card.component_id; 
/

create index obs_clin_idx on pcornet_cdm.obs_clin (PATID, ENCOUNTERID)
/

BEGIN
GATHER_TABLE_STATS('OBS_CLIN');
END;
/
                    
update cdm_status
set end_time = sysdate, records = (select count(*) from obs_clin)
where task = 'obs_clin'
/
