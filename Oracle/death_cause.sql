/** death_cause - create the death_cause table.
*/
insert into cdm_status (task, start_time) select 'death_cause', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE death_cause');
END;
/
CREATE TABLE death_cause(
	PATID varchar(50) NOT NULL,
	DEATH_CAUSE varchar(8) NOT NULL,
	DEATH_CAUSE_CODE varchar(2) NOT NULL,
	DEATH_CAUSE_TYPE varchar(2) NOT NULL,
	DEATH_CAUSE_SOURCE varchar(2) NOT NULL,
	DEATH_CAUSE_CONFIDENCE varchar(2) NULL
)
/
update cdm_status
set end_time = sysdate, records = (select count(*) from death_cause)
where task = 'death_cause'
/
select 1 from cdm_status where status = 'death_cause'
--SELECT count(PATID) from death_cause where rownum = 1