--------------------------------------------------------------------------------
-- MED_ADMIN
--------------------------------------------------------------------------------

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
    MEDADMIN_DOSE_ADMIN NUMBER(18, 0) NULL, -- (8,0)
    MEDADMIN_DOSE_ADMIN_UNIT varchar(50) NULL,
    MEDADMIN_ROUTE varchar(50) NULL,
    MEDADMIN_SOURCE varchar(2) NULL,
    RAW_MEDADMIN_MED_NAME varchar(50) NULL,
    RAW_MEDADMIN_CODE varchar(50) NULL,
    RAW_MEDADMIN_DOSE_ADMIN varchar(50) NULL,
    RAW_MEDADMIN_DOSE_ADMIN_UNIT varchar(50) NULL,
    RAW_MEDADMIN_ROUTE varchar(50) NULL
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

insert into med_admin(patid, encounterid, medadmin_providerid, medadmin_start_date, medadmin_start_time, medadmin_stop_date,
medadmin_stop_time, medadmin_type, medadmin_code, medadmin_source)
select patient_num, encounter_num, provider_id, start_date, to_char(start_date, 'HH24:MI'), end_date, to_char(end_date, 'HH24:MI'),
'RX' as medadmin_type, concept_cd, 'OD' as medadmin_source
from pcornet_cdm.observation_fact_meds;

execute immediate 'create index med_admin_idx on prescribing (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('MED_ADMIN');

end PCORNetMedAdmin;
/

SELECT 1 FROM HARVEST