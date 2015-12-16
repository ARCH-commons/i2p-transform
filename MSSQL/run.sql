-- Instructions: 
-- 1) Alter the database names to match yours in the places indicated.
-- 2) The loyalty cohort identification script must be run first. Note that the date windows in the loyalty cohort script are
--   presently hard-coded in both the loyalty cohort script and the PCORnetLoader script
-- 3) The PCORnetLoader_v6 script must be run first also
-- 4) Finally, be sure you have run the meds schemachange script on your medications ontology to create the additional columns.
-- 5) For testing, change the 100000000 number to something small, like 10000
-- 4) Run this from the database with the PopMedNet transforms and tables.   
--    Note that it could take a long time to run. (Should take ~30min per 10k patients, so about 1 day per 500k patients.)
--    NOTE (12-9-15) now the transform runs each procedure individually, to give the administrator finer-grained control. The old way still works too.
--    
-- All data from 1-1-2010 is transformed.
-- Jeff Klann, PhD


drop table i2b2patient_list
GO

-- Make 100000000 number smaller for testing
select distinct top 100000000 patient_num into i2b2patient_list from i2b2fact where start_date>='20100101'
GO
-- Change to match your database names
drop synonym i2b2patient;
GO
drop view i2b2patient;
GO
-- Change to match your database name
create view i2b2patient as select * from PCORI_Mart..patient_dimension where patient_num in (select patient_num from i2b2patient_list)
GO
drop synonym i2b2visit;
GO
drop view i2b2visit;
GO
-- Change to match your database name
create view i2b2visit as select * from PCORI_Mart..visit_dimension where start_date>='20100101' and (end_date is null or end_date<getdate());
GO

--exec pcornetloader;
--GO
-- Now run each procedure individually to ease commenting/uncommenting 
exec pcornetclear
GO
exec PCORNetHarvest
GO
exec PCORNetDemographics
GO
exec PCORNetEncounter
GO
exec PCORNetDiagnosis
GO
exec PCORNetCondition
GO
exec PCORNetProcedure
GO
exec PCORNetVital
GO
exec PCORNetEnroll
GO
exec PCORNetLabResultCM
GO
exec PCORNetPrescribing
GO
exec PCORNetDispensing
GO
exec pcornetreport
GO

select * from i2pReport;


