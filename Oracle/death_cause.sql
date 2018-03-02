/** death_cause - create the death_cause table.
*/
select synonym_name from all_synonyms where 'dep' = 'pcornet_init.sql'
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
insert into cdm_status (status, last_update) values ('death_cause', sysdate)
/
select 1 from cdm_status where status = 'death_cause'
--SELECT count(PATID) from death_cause where rownum = 1