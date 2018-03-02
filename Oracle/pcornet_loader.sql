select patid from death_cause where 'dep' = 'death_cause.sql'
/
select patid from enrollment where 'dep' = 'enrollment.sql'
/
select networkid from harvest where 'dep' = 'harvest.sql'
/
select medadminid from med_admin where 'dep' = 'med_admin.sql'
/
select patid from pcornet_trial where 'dep' = 'pcornet_trial.sql'
/
select pro_cm_id from pro_cm where 'dep' = 'pro_cm.sql'
/
insert into cdm_status (status, last_update) values ('pcornet_loader', sysdate )
/
select 1 from cdm_status where status = 'pcornet_loader'