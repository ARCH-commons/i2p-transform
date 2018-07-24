/** encounter - create and populate the encounter table.
*/
insert into cdm_status (task, start_time) select 'encounter', sysdate from dual
/

BEGIN
PMN_DROPSQL('DROP TABLE encounter');
END;
/

BEGIN
PMN_DROPSQL('DROP TABLE drg');
END;
/

BEGIN
PMN_DROPSQL('drop table encounter_key');
END;
/

BEGIN
PMN_DROPSQL('drop table encounter_w_drg');
END;
/

BEGIN
PMN_DROPSQL('drop table encounter_w_type');
END;
/

BEGIN
PMN_DROPSQL('drop table encounter_w_pay');
END;
/

BEGIN
PMN_DROPSQL('drop table encounter_w_fin');
END;
/

create table drg as
select * from
(select patient_num,encounter_num,drg_type, drg,row_number() over (partition by  patient_num, encounter_num order by drg_type desc) AS rn from
(select patient_num,encounter_num,drg_type,max(drg) drg  from
(select distinct f.patient_num,encounter_num,SUBSTR(c_fullname,22,2) drg_type,SUBSTR(pcori_basecode,INSTR(pcori_basecode, ':')+1,3) drg from i2b2fact f
inner join demographic d on f.patient_num=d.patid
inner join pcornet_enc enc on enc.c_basecode  = f.concept_cd
and enc.c_fullname like '\PCORI\ENCOUNTER\DRG\%') drg1 group by patient_num,encounter_num,drg_type) drg) drg
where rn=1
/

create index drg_idx on drg (patient_num, encounter_num)
/

create table encounter_key as
select patient_num PATID
, encounter_num ENCOUNTERID
, start_date ADMIT_DATE
, end_Date DISCHARGE_DATE
, providerid PROVIDERID
, admitting_source
, discharge_disposition RAW_DISCHARGE_DISPOSITION
, discharge_status RAW_DISCHARGE_STATUS
, inout_cd
from i2b2visit v
join demographic d on v.patient_num=d.patid
/

create table encounter_w_drg as
select en.*
, drg DRG
, drg_type DRG_TYPE
from encounter_key en
left join drg on drg.patient_num = en.patid and drg.encounter_num = en.encounterid
/

create table encounter_w_type as
select en.*
, case when SUBSTR(pcori_basecode, INSTR(pcori_basecode, ':') + 1, 2) is not null then SUBSTR(pcori_basecode, INSTR(pcori_basecode, ':') + 1, 2) else 'UN' end ENC_TYPE
, case when SUBSTR(pcori_basecode, INSTR(pcori_basecode, ':') + 1, 2) = 'AV' then 'NI' else raw_discharge_disposition end DISCHARGE_DISPOSITION
, case when SUBSTR(pcori_basecode, INSTR(pcori_basecode, ':') + 1, 2) = 'AV' then 'NI' else raw_discharge_status end DISCHARGE_STATUS
from encounter_w_drg en
left join pcornet_enc e on e.c_dimcode like '%'''||inout_cd||'''%' and e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%'
/

create table encounter_w_pay as
select en.*
, f.instance_num
, f.tval_char RAW_PAYER_NAME_PRIMARY
, f.concept_cd RAW_PAYER_ID_PRIMARY
from pcornet_cdm.encounter_w_type en
left join i2b2fact f on f.patient_num = en.patid and f.encounter_num = en.encounterid and f.concept_cd like 'O2|PAYER_PRIMARY:%'
-- IDX fails, as multiple payers are mapped to a single encounter_num.
-- TODO: determine the rollup logic used in heron to identify the primary encounter.
-- and (f.concept_cd like 'O2|PAYER_PRIMARY:%' or 'IDX|PAYER_PRIMARY:%')
/

create table encounter_w_fin as
select en.*
, pm.code PAYER_TYPE_PRIMARY
, sf.tval_char RAW_PAYER_TYPE_PRIMARY
from pcornet_cdm.encounter_w_pay en
left join blueherondata.supplemental_fact sf on en.instance_num = sf.instance_num
left join pcornet_cdm.payer_map pm on (pm.payer_name = en.raw_payer_name_primary and en.raw_payer_id_primary like 'O2|PAYER_PRIMARY:%'
and pm.financial_class = sf.tval_char)
-- IDX doesn't provide a financial class.  Could be eliminated from the O2 mapping but it's actually more informative than
-- the payer name for deciding the payer type.
--or (pm.payer_name = en.raw_payer_name_primary and en.raw_payer_id_primary like 'IDX|PAYER_PRIMARY:%')
/

create table encounter as
select en.patid
, en.encounterid
, en.admit_date
, to_char(en.admit_date, 'HH24:MI') ADMIT_TIME
, en.discharge_date
, to_char(en.discharge_date,'HH24:MI') DISCHARGE_TIME
, en.providerid
, cast(null as varchar(3)) FACILITY_LOCATION
, en.enc_type
, cast(null as varchar(50)) FACILITYID
, en.discharge_disposition
, en.discharge_status
, en.drg
, en.drg_type
, en.admitting_source
, en.payer_type_primary
, cast(null as varchar(5)) PAYER_TYPE_SECONDARY
, cast(null as varchar(50)) FACILITY_TYPE
, cast(null as varchar(50)) RAW_SITEID
, cast(null as varchar(50)) RAW_ENC_TYPE
, cast(null as varchar(50)) RAW_DISCHARGE_DISPOSITION
, cast(null as varchar(50)) RAW_DISCHARGE_STATUS
, cast(null as varchar(50)) RAW_DRG_TYPE
, cast(null as varchar(50)) RAW_ADMITTING_SOURCE
, cast(null as varchar(50)) RAW_FACILITY_TYPE
, en.raw_payer_type_primary
, en.raw_payer_name_primary
, en.raw_payer_id_primary
, cast(null as varchar(50)) RAW_PAYER_TYPE_SECONDARY
, cast(null as varchar(50)) RAW_PAYER_NAME_SECONDARY
, cast(null as varchar(50)) RAW_PAYER_ID_SECONDARY
from encounter_w_fin en
/

create unique index encounter_pk on encounter (ENCOUNTERID)
/

create index encounter_idx on encounter (PATID, ENCOUNTERID)
/

BEGIN
GATHER_TABLE_STATS('ENCOUNTER');
END;
/

update cdm_status
set end_time = sysdate, records = (select count(*) from encounter)
where task = 'encounter'
/

select records from cdm_status where task = 'encounter'