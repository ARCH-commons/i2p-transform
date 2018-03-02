/** procedures - create and populate the procedures table.
*/

select encounterid from encounter where 'dep' = 'encounter.sql'
/
BEGIN
PMN_DROPSQL('DROP TABLE procedures');
END;
/
CREATE TABLE procedures(
	PROCEDURESID varchar(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID varchar(50) NOT NULL,
	ENC_TYPE varchar(2) NULL,
	ADMIT_DATE date NULL,
	PROVIDERID varchar(50) NULL,
	PX_DATE date NULL,
	PX varchar(11) NOT NULL,
	PX_TYPE varchar(2) NOT NULL,
	PX_SOURCE varchar(2) NULL,
	RAW_PX varchar(50) NULL,
	RAW_PX_TYPE varchar(50) NULL
)
/
BEGIN
PMN_DROPSQL('DROP sequence  procedures_seq');
END;
/
create sequence  procedures_seq
/
create or replace trigger procedures_trg
before insert on procedures
for each row
begin
  select procedures_seq.nextval into :new.PROCEDURESID from dual;
end;
/
create or replace procedure PCORNetProcedure as
begin

PMN_DROPSQL('drop index procedures_idx');

execute immediate 'truncate table procedures';

insert into procedures(
				patid,			encounterid,	enc_type, admit_date, px_date, providerid, px, px_type, px_source)
select  distinct fact.patient_num, enc.encounterid,	enc.enc_type, enc.admit_date, fact.start_date,
		fact.provider_id, SUBSTR(pr.pcori_basecode,INSTR(pr.pcori_basecode, ':')+1,11) px, SUBSTR(pr.c_fullname,18,2) pxtype,
    -- All are billing for now - see https://informatics.gpcnetwork.org/trac/Project/ticket/491
    'BI' px_source
from i2b2fact fact
 inner join	pcornet_proc pr on pr.c_basecode  = fact.concept_cd
 inner join encounter enc on enc.patid = fact.patient_num and enc.encounterid = fact.encounter_Num
where pr.c_fullname like '\PCORI\PROCEDURE\%';

execute immediate 'create index procedures_idx on procedures (PATID, ENCOUNTERID)';
--GATHER_TABLE_STATS('PROCEDURES');

end PCORNetProcedure;
/
insert into cdm_status (status, last_update) values ('procedures', sysdate)
--BEGIN
--PCORNetProcedure();
--END;
/
select 1 from cdm_status where status = 'procedures'
--SELECT count(PATID) from procedures where rownum = 1