/** med_admin - create and populate the med_admin table.
*/
insert into cdm_status (task, start_time) select 'med_admin', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE med_admin');
END;
/
CREATE TABLE med_admin(
    MEDADMINID varchar(50) primary key,
    PATID varchar(50) NOT NULL,
    ENCOUNTERID varchar(50) NULL,
    PRESCRIBINGID varchar(50) NULL,
    MEDADMIN_PROVIDERID varchar(50) NULL,
    MEDADMIN_START_DATE date NOT NULL,
    MEDADMIN_START_TIME varchar(5) NULL,
    MEDADMIN_STOP_DATE date NULL,
    MEDADMIN_STOP_TIME varchar(5) NULL,
    MEDADMIN_TYPE varchar(2) NULL,
    MEDADMIN_CODE varchar(50) NULL,
    MEDADMIN_DOSE_ADMIN NUMBER(18, 5) NULL, -- (8,0)
    MEDADMIN_DOSE_ADMIN_UNIT varchar(50) NULL,
    MEDADMIN_ROUTE varchar(50) NULL,
    MEDADMIN_SOURCE varchar(2) NULL,
    RAW_MEDADMIN_MED_NAME varchar(2000) NULL,
    RAW_MEDADMIN_CODE varchar(50) NULL,
    RAW_MEDADMIN_DOSE_ADMIN varchar(50) NULL,
    RAW_MEDADMIN_DOSE_ADMIN_UNIT varchar(50) NULL,
    RAW_MEDADMIN_ROUTE varchar(100) NULL
)
/
BEGIN
PMN_DROPSQL('DROP sequence med_admin_seq');
END;
/
create sequence med_admin_seq
/
create or replace trigger med_admin_trg
before insert on med_admin
for each row
begin
  select med_admin_seq.nextval into :new.MEDADMINID from dual;
end;
/
create or replace procedure PCORNetMedAdmin as
begin

PMN_DROPSQL('drop index med_admin_idx');

execute immediate 'truncate table med_admin';

insert into med_admin(patid
  , encounterid
  , prescribingid
  , medadmin_providerid
  , medadmin_start_date
  , medadmin_start_time
  , medadmin_stop_date
  , medadmin_stop_time
  , medadmin_type
  , medadmin_code
  , medadmin_dose_admin
  , medadmin_dose_admin_unit
  , medadmin_route
  , medadmin_source
  , raw_medadmin_med_name
  , raw_medadmin_code
  , raw_medadmin_dose_admin
  , raw_medadmin_dose_admin_unit
  , raw_medadmin_route)
with med_start as (
    select patient_num, encounter_num, provider_id, start_date, end_date, concept_cd, modifier_cd, instance_num
    from BLUEHERONDATA.observation_fact
    where modifier_cd = 'MedObs|MAR:New Bag'
    or modifier_cd = 'MedObs|MAR:Downtime Given - New Bag'
    or modifier_cd = 'MedObs|MAR:Given -  Without Order'
    or modifier_cd = 'MedObs|MAR:Downtime Given'
    or modifier_cd = 'MedObs|MAR:Given - Without Order'
    or modifier_cd = 'MedObs|MAR:Given by another Provider'
    or modifier_cd = 'MedObs|MAR:Given by Patient/Family'
    or modifier_cd = 'MedObs|MAR:Given'
    or modifier_cd = 'MedObs|MAR:Clinic Administered - Patient Supplied'
    or modifier_cd = 'MedObs|MAR:Bolus'
    or modifier_cd = 'MedObs|MAR:Per Protocol'
    or modifier_cd = 'MedObs|MAR:Infusion Home with Patient'
    or modifier_cd = 'MedObs|MAR:ED-Infusing Upon Admit'
    or modifier_cd = 'MedObs|MAR:Push'
    or modifier_cd = 'MedObs|MAR:Restarted'
    or modifier_cd = 'MedObs|MAR:PCA Check/Change'
    or modifier_cd = 'MedObs|MAR:NPO'
    or modifier_cd = 'MedObs|MAR:See OR/Proc Flowsheet'
    or modifier_cd = 'MedObs|MAR:Patch Applied'
    or modifier_cd = 'MedObs|MAR:Bolus from Syringe'
    or modifier_cd = 'MedObs|MAR:Bolus.'
)
select med_start.patient_num
  , med_start.encounter_num
  , null
  , med_start.provider_id
  , med_start.start_date
  , to_char(med_start.start_date, 'HH24:MI')
  , med_start.end_date
  , to_char(med_start.end_date, 'HH24:MI')
  , 'RX'
  , med_p.pcori_basecode
  , med_dose.nval_num
  , case when nval_num is null then null else nvl(um.code, 'OT') end
  , null
  , 'OD'
  , med_p.c_name
  , med_start.concept_cd
  , med_dose.nval_num
  , med_dose.units_cd
  , med_start.modifier_cd -- Modifier code rather than raw route.
from med_start
left join BLUEHERONDATA.observation_fact med_dose
on med_dose.instance_num = med_start.instance_num
and med_dose.start_date = med_start.start_date
and (med_dose.modifier_cd = 'MedObs:MAR_Dose|puff'
or med_dose.modifier_cd = 'MedObs:MAR_Dose|cap'
or med_dose.modifier_cd = 'MedObs:MAR_Dose|drop'
or med_dose.modifier_cd = 'MedObs:MAR_Dose|meq'
or med_dose.modifier_cd = 'MedObs:MAR_Dose|tab'
or med_dose.modifier_cd = 'MedObs:MAR_Dose|units'
or med_dose.modifier_cd = 'MedObs:MAR_Dose|l'
or med_dose.modifier_cd = 'MedObs:MAR_Dose|mg'
--or med_dose.modifier_cd = 'MedObs:Dose|puff'
--or med_dose.modifier_cd = 'MedObs:Dose|drop'
--or med_dose.modifier_cd = 'MedObs:Dose|cap'
--or med_dose.modifier_cd = 'MedObs:Dose|meq'
--or med_dose.modifier_cd = 'MedObs:Dose|units'
--or med_dose.modifier_cd = 'MedObs:Dose|l'
--or med_dose.modifier_cd = 'MedObs:Dose|tab'
--or med_dose.modifier_cd = 'MedObs:Dose|mg'
)
left join BLUEHERONMETADATA.pcornet_med med_p on med_p.c_basecode = med_start.concept_cd
left join unit_map um on um.unit_name = med_dose.units_cd
;

execute immediate 'create index med_admin_idx on med_admin (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('MED_ADMIN');

end PCORNetMedAdmin;
/
BEGIN
PCORNetMedAdmin();
END;
/
update cdm_status
set end_time = sysdate, records = (select count(*) from med_admin)
where task = 'med_admin'
/
select records from cdm_status where task = 'med_admin'
