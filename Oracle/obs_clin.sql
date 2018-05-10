/** obs_gen - create the obs_clin table.
*/
insert into cdm_status (task, start_time) select 'obs_clin', sysdate from dual
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
    RAW_OBSCLIN_NAME varchar(50) NULL,
    RAW_OBSCLIN_CODE varchar(50) NULL,
    RAW_OBSCLIN_TYPE varchar(50) NULL,
    RAW_OBSCLIN_RESULT varchar(50) NULL,
    RAW_OBSCLIN_MODIFIER varchar(50) NULL,
    RAW_OBSCLIN_UNIT varchar(50) NULL
)
/
update cdm_status
set end_time = sysdate, records = (select count(*) from obs_clin)
where task = 'obs_clin'
/
select 1 from cdm_status where status = 'obs_clin'