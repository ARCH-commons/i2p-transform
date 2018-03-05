/** enrollment - create and populate the enrollment table.
*/
select synonym_name from all_synonyms where 'dep' = 'pcornet_init.sql'
/
BEGIN
PMN_DROPSQL('DROP TABLE enrollment');
END;
/
CREATE TABLE enrollment (
	PATID varchar(50) NOT NULL,
	ENR_START_DATE date NOT NULL,
	ENR_END_DATE date NULL,
	CHART varchar(1) NULL,
	ENR_BASIS varchar(1) NOT NULL,
	RAW_CHART varchar(50) NULL,
	RAW_BASIS varchar(50) NULL
)
/
create or replace procedure PCORNetEnroll as
begin

PMN_DROPSQL('drop index enrollment_idx');

execute immediate 'truncate table enrollment';

INSERT INTO enrollment(PATID, ENR_START_DATE, ENR_END_DATE, CHART, ENR_BASIS)
with pats_delta as (
  -- If only one visit, visit_delta_days will be 0
  select patient_num, max(start_date) - min(start_date) visit_delta_days
  from i2b2visit
  where start_date > add_months(sysdate, -&&enrollment_months_back)
  group by patient_num
  ),
enrolled as (
  select distinct patient_num
  from pats_delta
  where visit_delta_days > 30
  )
select
  visit.patient_num patid, min(visit.start_date) enr_start_date,
  max(visit.start_date) enr_end_date, 'Y' chart, 'A' enr_basis
from enrolled enr
join i2b2visit visit on enr.patient_num = visit.patient_num
group by visit.patient_num;

execute immediate 'create index enrollment_idx on enrollment (PATID)';
GATHER_TABLE_STATS('ENROLLMENT');

end PCORNetEnroll;
/
BEGIN
PCORNetEnroll();
END;
/
insert into cdm_status (status, last_update) values ('enrollment', sysdate)
/
select 1 from cdm_status where status = 'enrollment'
--SELECT count(PATID) from enrollment where rownum = 1