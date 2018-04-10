insert into cdm_status (status, last_update) values ('pcornet_loader', sysdate )
/
select 1 from cdm_status where status = 'pcornet_loader'