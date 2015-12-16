-- Instructions: 
-- 1) Alter the database names to match yours in the places indicated.
-- 2) The loyalty cohort identification script must be run first
-- 3) Run this from the database with the PopMedNet transforms and tables.  
--    Note that it could take several hours to run.

-- Simple script to execute pcornet loader on a subset of patients
-- This chooses patients that were selected by the loyalty cohort script which match
-- the following filters: ('AgeSex','Race','Alive','SameState','FirstLast18Months','NoSmallFactCount','Diagnoses','AgeCutoffs')
-- All data from 1-1-2010 is transformed.
-- The code '61511' in the filter selection can be reconstituted through the following SQL code:
/*
-- This query is how you determine the filter set should be 61511
select sum(filter_bit) 
from loyalty_cohort_filters
where description in ('AgeSex','Race','Alive','SameState','FirstLast18Months','NoSmallFactCount','Diagnoses','AgeCutoffs')
*/
-- Jeff Klann, PhD

drop table i2b2patient_list;

-- Change to match your database names
-- The first PCORI_Mart should be the database where the loyalty cohort summary tables reside
-- The second PCORI_Mart should be your SCILHS i2b2 database 
create table i2b2patient_list
as
select * from 
(
select patient_num from I2B2DEMODATA.LOYALTY_COHORT_PATIENT_SUMMARY where BITAND(LOYALTY_COHORT_PATIENT_SUMMARY.FILTER_SET, 61511)=61511
) where rownum <=1000000
;

drop synonym i2b2patient;

drop view i2b2patient;

-- Change to match your database name
create or replace view i2b2patient as select * from i2b2demodata.patient_dimension where patient_num in (select patient_num from I2B2PATIENT_LIST);

drop synonym i2b2visit;

drop view i2b2visit;

-- Change to match your database name
create or replace view i2b2visit as select * from I2B2DEMODATA.visit_dimension where start_date >= to_date('01-01-2010', 'DD-MM-YYYY');

begin
	pcornetloader();
end;

select * from i2pReport;

select concept "Data Type",sourceval "From i2b2",destval "In PopMedNet", diff "Difference" from i2preport where runid=(select max(runid) from i2preport);