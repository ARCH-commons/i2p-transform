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
, admitting_source ADMITTING_SOURCE
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
, nvl(SUBSTR(pcori_basecode, INSTR(pcori_basecode, ':') + 1, 2), 'UN') ENC_TYPE
, case when SUBSTR(pcori_basecode, INSTR(pcori_basecode, ':') + 1, 2) = 'AV' then 'NI' else raw_discharge_disposition end DISCHARGE_DISPOSITION
, case when SUBSTR(pcori_basecode, INSTR(pcori_basecode, ':') + 1, 2) = 'AV' then 'NI' else raw_discharge_status end DISCHARGE_STATUS
from encounter_w_drg en
left join pcornet_enc e on e.c_dimcode like '%'''||inout_cd||'''%' and e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%'
/

-- pay_order was added as a quick fix for the problem of multiple payers mapping to the same encounterid.
-- The partition / order by operation selects the primary payer by alphabetical order (not good).
-- Other options considered were precedence by encounter type, by financial class or by CDM code but
-- none proved clear and expedient.
-- TODO: rework payer selection to align with encounter rollup logic or determine a clear precedence from other criteria.
create table encounter_w_pay as
select en.*
, pay.instance_num
, pay.tval_char RAW_PAYER_NAME_PRIMARY
, pay.concept_cd RAW_PAYER_ID_PRIMARY
from encounter_w_type en
left join
  (select encounter_num
  , patient_num
  , instance_num
  , concept_cd
  , tval_char
  , row_number() over (partition by patient_num, encounter_num order by tval_char) as pay_order
  from i2b2fact
  where concept_cd like 'O2|PAYER_PRIMARY:%' or concept_cd like 'IDX|PAYER_PRIMARY:%'
  ) pay on en.patid = pay.patient_num and en.encounterid = pay.encounter_num and pay_order = 1
/

-- IDX does not provide a financial class, hence the abbreviated condition.  However, the payer type code can be
-- inferred from the payer name, particularly were IDX names and O2 names coincide.  This might challenge the
-- do not impute rule specified in the CDM spec, but doesn't seem unreasonable.
select en.*
, pm.code PAYER_TYPE_PRIMARY
, sf.tval_char RAW_PAYER_TYPE_PRIMARY
from encounter_w_pay en
left join &&i2b2_data_schema.supplemental_fact sf on en.instance_num = sf.instance_num and sf.source_column = 'FINANCIAL_CLASS'
left join payer_map pm on pm.payer_name = en.raw_payer_name_primary
and (en.raw_payer_id_primary like 'O2|PAYER_PRIMARY:%' and pm.financial_class = sf.tval_char)
or (en.raw_payer_id_primary like 'IDX|PAYER_PRIMARY:%')
/

create table encounter as
select cast(patid as varchar(50)) PATID
, cast(encounterid as varchar(50)) ENCOUNTERID
, en.admit_date
, to_char(en.admit_date, 'HH24:MI') ADMIT_TIME
, en.discharge_date
, to_char(en.discharge_date,'HH24:MI') DISCHARGE_TIME
, en.providerid
, cast('NI' as varchar(3)) FACILITY_LOCATION
, en.enc_type
, cast('NI' as varchar(50)) FACILITYID
, cast(en.discharge_disposition as varchar(2)) DISCHARGE_DISPOSITION
, cast(en.discharge_status as varchar(2)) DISCHARGE_STATUS
, en.drg
, en.drg_type
, cast(en.admitting_source as varchar(2)) ADMITTING_SOURCE
, cast(en.payer_type_primary as varchar(5)) PAYER_TYPE_PRIMARY
, cast(null as varchar(5)) PAYER_TYPE_SECONDARY
, cast('NI' as varchar(50)) FACILITY_TYPE
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