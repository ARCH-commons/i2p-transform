/** pcornet_trial - create the pcornet_trial table.
*/
select synonym_name from all_synonyms where 'dep' = 'pcornet_init.sql'
/
BEGIN
PMN_DROPSQL('DROP TABLE pcornet_trial');
END;
/
CREATE TABLE pcornet_trial(
	PATID varchar(50) NOT NULL,
	TRIALID varchar(20) NOT NULL,
	PARTICIPANTID varchar(50) NOT NULL,
	TRIAL_SITEID varchar(50) NULL,
	TRIAL_ENROLL_DATE date NULL,
	TRIAL_END_DATE date NULL,
	TRIAL_WITHDRAW_DATE date NULL,
	TRIAL_INVITE_CODE varchar(20) NULL
)
/
insert into cdm_status (status, last_update, records) select 'pcornet_trial', sysdate, count(*) from pcornet_trial
/
select 1 from cdm_status where status = 'pcornet_trial'