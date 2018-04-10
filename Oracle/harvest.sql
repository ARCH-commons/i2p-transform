/** harvest - create and populate the harvest table.
*/
BEGIN
PMN_DROPSQL('DROP TABLE harvest');
END;
/
CREATE TABLE harvest(
	NETWORKID varchar(10) NOT NULL,
	NETWORK_NAME varchar(20) NULL,
	DATAMARTID varchar(10) NOT NULL,
	DATAMART_NAME varchar(20) NULL,
	DATAMART_PLATFORM varchar(2) NULL,
	CDM_VERSION numeric(8, 2) NULL,
	DATAMART_CLAIMS varchar(2) NULL,
	DATAMART_EHR varchar(2) NULL,
	BIRTH_DATE_MGMT varchar(2) NULL,
	ENR_START_DATE_MGMT varchar(2) NULL,
	ENR_END_DATE_MGMT varchar(2) NULL,
	ADMIT_DATE_MGMT varchar(2) NULL,
	DISCHARGE_DATE_MGMT varchar(2) NULL,
	PX_DATE_MGMT varchar(2) NULL,
	RX_ORDER_DATE_MGMT varchar(2) NULL,
	RX_START_DATE_MGMT varchar(2) NULL,
	RX_END_DATE_MGMT varchar(2) NULL,
	DISPENSE_DATE_MGMT varchar(2) NULL,
	LAB_ORDER_DATE_MGMT varchar(2) NULL,
	SPECIMEN_DATE_MGMT varchar(2) NULL,
	RESULT_DATE_MGMT varchar(2) NULL,
	MEASURE_DATE_MGMT varchar(2) NULL,
	ONSET_DATE_MGMT varchar(2) NULL,
	REPORT_DATE_MGMT varchar(2) NULL,
	RESOLVE_DATE_MGMT varchar(2) NULL,
	PRO_DATE_MGMT varchar(2) NULL,
	DEATH_DATE_MGTM varchar(2) NULL,
	MEDADMIN_START_DATE_MGMT varchar(2) NULL,
	MEDADMIN_END_DATE_MGMT varchar(2) NULL,
	OBSCLIN_DATE_MGMT varchar(2) NULL,
	OBSGEN_DATE_MGMT varchar(2) NULL,
	REFRESH_DEMOGRAPHIC_DATE date NULL,
	REFRESH_ENROLLMENT_DATE date NULL,
	REFRESH_ENCOUNTER_DATE date NULL,
	REFRESH_DIAGNOSIS_DATE date NULL,
	REFRESH_PROCEDURES_DATE date NULL,
	REFRESH_VITAL_DATE date NULL,
	REFRESH_DISPENSING_DATE date NULL,
	REFRESH_LAB_RESULT_CM_DATE date NULL,
	REFRESH_CONDITION_DATE date NULL,
	REFRESH_PRO_CM_DATE date NULL,
	REFRESH_PRESCRIBING_DATE date NULL,
	REFRESH_PCORNET_TRIAL_DATE date NULL,
	REFRESH_DEATH_DATE date NULL,
	REFRESH_DEATH_CAUSE_DATE date NULL,
	REFRESH_MED_ADMIN_DATE date NULL,
	REFRESH_OBS_CLIN_DATE date NULL,
	REFRESH_PROVIDER_DATE date NULL,
	REFRESH_OBS_GEN_DATE date NULL
)
/
create or replace procedure PCORNetHarvest as
begin

execute immediate 'truncate table harvest';

INSERT INTO harvest(NETWORKID, NETWORK_NAME, DATAMARTID, DATAMART_NAME, DATAMART_PLATFORM, CDM_VERSION, DATAMART_CLAIMS, DATAMART_EHR,
    BIRTH_DATE_MGMT, ENR_START_DATE_MGMT, ENR_END_DATE_MGMT, ADMIT_DATE_MGMT, DISCHARGE_DATE_MGMT, PX_DATE_MGMT, RX_ORDER_DATE_MGMT,
    RX_START_DATE_MGMT, RX_END_DATE_MGMT, DISPENSE_DATE_MGMT, LAB_ORDER_DATE_MGMT, SPECIMEN_DATE_MGMT, RESULT_DATE_MGMT, MEASURE_DATE_MGMT,
    ONSET_DATE_MGMT, REPORT_DATE_MGMT, RESOLVE_DATE_MGMT, PRO_DATE_MGMT, DEATH_DATE_MGTM, MEDADMIN_START_DATE_MGMT, MEDADMIN_END_DATE_MGMT,
    OBSCLIN_DATE_MGMT, OBSGEN_DATE_MGMT, REFRESH_DEMOGRAPHIC_DATE, REFRESH_ENROLLMENT_DATE,
    REFRESH_ENCOUNTER_DATE, REFRESH_DIAGNOSIS_DATE, REFRESH_PROCEDURES_DATE, REFRESH_VITAL_DATE, REFRESH_DISPENSING_DATE,
    REFRESH_LAB_RESULT_CM_DATE, REFRESH_CONDITION_DATE, REFRESH_PRO_CM_DATE, REFRESH_PRESCRIBING_DATE, REFRESH_PCORNET_TRIAL_DATE,
    REFRESH_DEATH_DATE, REFRESH_DEATH_CAUSE_DATE, REFRESH_MED_ADMIN_DATE, REFRESH_OBS_CLIN_DATE, REFRESH_PROVIDER_DATE, REFRESH_OBS_GEN_DATE)
	select '&&network_id', '&&network_name', getDataMartID(), getDataMartName(), getDataMartPlatform(), 3, hl.DATAMART_CLAIMS, hl.DATAMART_EHR,
	hl.BIRTH_DATE_MGMT, hl.ENR_START_DATE_MGMT, hl.ENR_END_DATE_MGMT, hl.ADMIT_DATE_MGMT, hl.DISCHARGE_DATE_MGMT, hl.PX_DATE_MGMT,
	hl.RX_ORDER_DATE_MGMT, hl.RX_START_DATE_MGMT, hl.RX_END_DATE_MGMT, hl.DISPENSE_DATE_MGMT, hl.LAB_ORDER_DATE_MGMT, hl.SPECIMEN_DATE_MGMT,
	hl.RESULT_DATE_MGMT, hl.MEASURE_DATE_MGMT, hl.ONSET_DATE_MGMT, hl.REPORT_DATE_MGMT, hl.RESOLVE_DATE_MGMT, hl.PRO_DATE_MGMT,
	hl.DEATH_DATE_MGMT, hl.MEDADMIN_START_DATE_MGMT, hl.MEDADMIN_END_DATE_MGMT, hl.OBSCLIN_DATE_MGMT, hl.OBSGEN_DATE_MGMT,
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
  case when (select count(*) from death_cause) > 0 then current_date else null end REFRESH_DEATH_CAUSE_DATE,
  case when (select count(*) from med_admin) > 0 then current_date else null end REFRESH_MED_ADMIN_DATE,
  case when (select count(*) from obs_clin) > 0 then current_date else null end REFRESH_OBS_CLIN_DATE,
  case when (select count(*) from provider) > 0 then current_date else null end REFRESH_PROVIDER_DATE,
  case when (select count(*) from obs_gen) > 0 then current_date else null end REFRESH_OBS_GEN_DATE

  from harvest_local hl;

end PCORNetHarvest;
/
BEGIN
PCORNetHarvest();
END;
/
insert into cdm_status (status, last_update, records) select 'harvest', sysdate, count(*) from harvest
/
select 1 from cdm_status where status = 'harvest'
--SELECT count(NETWORKID) from Harvest where rownum = 1