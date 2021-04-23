insert into cdm_status (task, start_time) select 'lab_history', sysdate from dual
/

BEGIN
PMN_DROPSQL('DROP TABLE lab_history');
END;
/

CREATE TABLE lab_history(
    LABHISTORYID varchar(50),
    LAB_LOINC varchar(10),
    LAB_FACILITYID varchar(50),
    SEX varchar(2),
    RACE varchar(2),
    AGE_MIN_WKS number,
    AGE_MAX_WKS number,
    RESULT_UNIT varchar(2),
    NORM_RANGE_LOW varchar(10),
    NORM_MODIFIER_HIGH varchar(2),
    NORM_MODIFIER_LOW varchar(2),
    NORM_RANGE_HIGH varchar(10),
    NORM_RANGE_MODIFIER_HIGH varchar(2),
    PERIOD_START DATE,
    PERIOD_END DATE,
    RAW_LAB_NAME varchar(50),
    RAW_UNIT varchar(50),
    RAW_RANGE varchar(50)
)
/
BEGIN
GATHER_TABLE_STATS('LAB_HISTORY');
END;
/

BEGIN
PMN_DROPSQL('DROP SEQUENCE lab_history_seq');
END;
/
create sequence  lab_history_seq cache 2000
/
create or replace trigger lab_history_trg
before insert on lab_history
for each row
begin
  select lab_history_seq.nextval into :new.LABHISTORYID from dual;
end;
/

update cdm_status
set end_time = sysdate, records = (select count(*) from lab_history)
where task = 'lab_history'
/
select records + 1 from cdm_status 
where task = 'lab_history' and records is not NULL
/
