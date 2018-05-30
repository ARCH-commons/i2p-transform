/** prescribing - create and populate the prescribing table.
*/
insert into cdm_status (task, start_time) select 'prescribing', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE prescribing');
END;
/
BEGIN
PMN_DROPSQL('drop table prescribing_key');
END;
/
BEGIN
PMN_DROPSQL('drop table prescribing_w_cui');
END;
/
BEGIN
PMN_DROPSQL('drop table prescribing_w_refills');
END;
/
BEGIN
PMN_DROPSQL('drop table prescribing_w_freq');
END;
/
BEGIN
PMN_DROPSQL('drop table prescribing_w_quantity');
END;
/
BEGIN
PMN_DROPSQL('drop table prescribing_w_supply');
END;
/
BEGIN
PMN_DROPSQL('drop table prescribing_w_basis');
END;
/
BEGIN
PMN_DROPSQL('DROP sequence  prescribing_seq');
END;
/

create sequence  prescribing_seq cache 2000
/

/** prescribing_key -- one row per order (inpatient or outpatient)

Design note on modifiers:
create or replace view rx_mod_design as
  select c_basecode
  from blueheronmetadata.pcornet_med
  -- TODO: fix pcornet_mapping.csv
  where c_fullname like '\PCORI_MOD\RX_BASIS\PR\_%'
  and c_basecode not in ('MedObs:PRN',   -- that's a supplementary flag, not a basis
                         'MedObs:Other'  -- that's as opposed to Historical; not same category as Inpatient, Outpatient
                         )
  and c_basecode not like 'RX_BASIS:%'
;

select case when (
  select listagg(c_basecode, ',') within group (order by c_basecode) modifiers
  from rx_mod_design
  ) = 'MedObs:Inpatient,MedObs:Outpatient'
then 1 -- pass
else 1/0 -- fail
end modifiers_as_expected
from dual
;
*/
create table prescribing_key as
select cast(prescribing_seq.nextval as varchar(19)) prescribingid
, instance_num
, cast(patient_num as varchar(50)) patient_num
, cast(encounter_num as varchar(50)) encounter_num
, provider_id
, start_date
, end_date
, concept_cd
, modifier_cd
from blueherondata.observation_fact rx
join encounter en on rx.encounter_num = en.encounterid
where rx.modifier_cd in ('MedObs:Inpatient', 'MedObs:Outpatient')
/

alter table prescribing_key modify (provider_id null)
/

/** prescribing_w_cui

take care with cardinalities...

select count(distinct c_name), c_basecode
from pcornet_med
group by c_basecode, c_name
having count(distinct c_name) > 1
order by c_basecode
;
*/
create table prescribing_w_cui as
select rx.*
, mo.pcori_cui rxnorm_cui
, substr(mo.c_basecode, 1, 50) raw_rxnorm_cui
, substr(mo.c_name, 1, 50) raw_rx_med_name
from prescribing_key rx
left join
  (select distinct c_basecode
  , c_name
  , pcori_cui
  from pcornet_med
  ) mo
on rx.concept_cd = mo.c_basecode
/

/** prescribing_w_refills
This join isn't guaranteed not to introduce more rows,
but at least one measurement showed it does not.
 */
create table prescribing_w_refills as
select rx.*
, refills.nval_num rx_refills
from prescribing_w_cui rx
left join
  (select instance_num
  , concept_cd
  , nval_num
  from blueherondata.observation_fact
  where modifier_cd = 'RX_REFILLS'
    /* aka:
    select c_basecode from pcornet_med refillscode
    where refillscode.c_fullname like '\PCORI_MOD\RX_REFILLS\' */
  ) refills on refills.instance_num = rx.instance_num and refills.concept_cd = rx.concept_cd
/

create table prescribing_w_freq as
select rx.*
, substr(freq.pcori_basecode, instr(freq.pcori_basecode, ':') + 1, 2) rx_frequency
from prescribing_w_refills rx
left join
  (select instance_num
  , concept_cd
  , pcori_basecode
  from blueherondata.observation_fact
  join pcornet_med on modifier_cd = c_basecode
  and c_fullname like '\PCORI_MOD\RX_FREQUENCY\%'
  ) freq on freq.instance_num = rx.instance_num and freq.concept_cd = rx.concept_cd
/

create table prescribing_w_quantity as
select rx.*
, quantity.nval_num rx_quantity
from prescribing_w_freq rx
left join
  (select instance_num
  , concept_cd
  , nval_num
  from blueherondata.observation_fact
  where modifier_cd = 'RX_QUANTITY'
    /* aka:
    select c_basecode from pcornet_med refillscode
    where refillscode.c_fullname like '\PCORI_MOD\RX_QUANTITY\' */
  ) quantity on quantity.instance_num = rx.instance_num and quantity.concept_cd = rx.concept_cd
/

create table prescribing_w_supply as
select rx.*
, supply.nval_num rx_days_supply
from prescribing_w_quantity rx
left join
  (select instance_num
  , concept_cd
  , nval_num
  from blueherondata.observation_fact
  where modifier_cd = 'RX_DAYS_SUPPLY'
    /* aka:
    select c_basecode from pcornet_med refillscode
    where refillscode.c_fullname like '\PCORI_MOD\RX_DAYS_SUPPLY\' */
  ) supply on supply.instance_num = rx.instance_num and supply.concept_cd = rx.concept_cd
/

create table prescribing_w_basis as
select rx.*
, substr(basis.pcori_basecode, instr(basis.pcori_basecode, ':') + 1, 2) rx_basis
from prescribing_w_supply rx
left join
  (select instance_num
  , concept_cd
  , pcori_basecode
  from blueherondata.observation_fact
  join pcornet_med on modifier_cd = c_basecode
  and c_fullname like '\PCORI_MOD\RX_BASIS\%'
  and modifier_cd in ('MedObs:Inpatient', 'MedObs:Outpatient')
  ) basis on basis.instance_num = rx.instance_num and basis.concept_cd = rx.concept_cd
/

create table prescribing_w_dose as
select rx.*
, units_cd rx_dose_ordered
, nval_num rx_dose_ordered_unit
from prescribing_w_basis rx
left join
  (select instance_num
  , concept_cd
  , case when units_cd = 'mcg' then 'ug' when units_cd = 'l' then 'ml' else units_cd end units_cd
  , case when units_cd = 'l' then nval_num * 1000 else nval_num end nval_num
  from blueherondata.observation_fact
  where modifier_cd in ('MedObs:Dose|mg', 'MedObs:Dose|meq', 'MedObs:Dose|l')
  ) dose on dose.instance_num = rx.instance_num and dose.concept_cd = rx.concept_cd
/

create table prescribing as
select rx.prescribingid
, rx.patient_num patid
, rx.encounter_num encounterid
, rx.provider_id rx_providerid
, trunc(rx.start_date) rx_order_date
, to_char(rx.start_date, 'HH24:MI') rx_order_time
, trunc(rx.start_date) rx_start_date
, trunc(rx.end_date) rx_end_date
, rx.rx_dose_ordered
, rx.rx_dose_ordered_unit
, rx.rx_quantity
, 'NI' rx_quantity_unit
, rx.rx_refills
, rx.rx_days_supply
, rx.rx_frequency
, decode(rx.modifier_cd, 'MedObs:Inpatient', '01', 'MedObs:Outpatient', '02') rx_basis
, rx.rxnorm_cui
, rx.raw_rx_med_name
, cast(null as varchar(50)) raw_rx_frequency
, cast(null as varchar(50)) raw_rx_quantity
, cast(null as varchar(50)) raw_rx_ndc
, rx.raw_rxnorm_cui
/* ISSUE: HERON should have an actual order time.
   idea: store real difference between order date start data, possibly using the update date */
from prescribing_w_dose rx
/

create index prescribing_idx on prescribing (PATID, ENCOUNTERID)
/
BEGIN
GATHER_TABLE_STATS('PRESCRIBING');
END;
/

update cdm_status
set end_time = sysdate, records = (select count(*) from prescribing)
where task = 'prescribing'
/

select records from cdm_status where task = 'prescribing'
