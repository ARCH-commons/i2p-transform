/** pcornet_trial - create the pcornet_trial table.
*/
insert into cdm_status (task, start_time) select 'pcornet_trail', sysdate from dual
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
update cdm_status
set end_time = sysdate, records = (select count(*) from pcornet_trial)
where task = 'pcornet_trial'
/
select 1 from cdm_status where status = 'pcornet_trial'