/** pro_cm - create the pro_cm table.
*/
insert into cdm_status (task, start_time) select 'pro_cm', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE pro_cm');
END;
/
CREATE TABLE pro_cm(
	PRO_CM_ID varchar(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID varchar(50) NULL,
	PRO_DATE date NOT NULL,
	PRO_TIME varchar(5) NULL,
	PRO_TYPE varchar(2) NULL,
    PRO_ITEM_NAME varchar(50) NULL,
    PRO_ITEM_LOINC varchar(10) NULL,
    PRO_RESPONSE_TEXT varchar(50) NULL,
    PRO_RESPONSE_NUM number(8) NOT NULL,
    PRO_METHOD varchar(2) NULL,
    PRO_MODE varchar(2) NULL,
	PRO_CAT varchar(2) NULL,
	PRO_ITEM_VERSION varchar(50) NULL,
	PRO_MEASURE_NAME varchar(50) NULL,
	PRO_MEASURE_SEQ varchar(50) NULL,
	PRO_MEASURE_SCORE number(8) NULL,
	PRO_MEASURE_THETA number(8) NULL,
	PRO_MEASURE_SCALED_TSCORE number(8) NULL,
	PRO_MEASURE_STANDARD_ERROR number(8) NULL,
	PRO_MEASURE_COUNT_SCORED number(8) NULL,
	PRO_MEASURE_LOINC varchar(10) NULL,
	PRO_MEASURE_VERSION varchar(50) NULL,
	PRO_ITEM_FULLNAME varchar(50) NULL,
	PRO_ITEM_TEXT varchar(50) NULL,
	PRO_MEASURE_FULLNAME varchar(50) NULL,
    PRO_SOURCE varchar(2) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  pro_cm_seq');
END;
/
create sequence  pro_cm_seq
/

create or replace trigger pro_cm_trg
before insert on pro_cm
for each row
begin
  select pro_cm_seq.nextval into :new.PRO_CM_ID from dual;
end;
/
update cdm_status
set end_time = sysdate, records = (select count(*) from pro_cm)
where task = 'pro_cm'
/
select records + 1 from cdm_status where task = 'pro_cm'
