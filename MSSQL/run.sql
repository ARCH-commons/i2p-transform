-- Instructions: 
-- 1) The PCORnetLoader script (05-24-17 version or later) must be run first 
-- 2) If you are using it, the loyalty cohort identification script must be run first. Note that the date windows in the loyalty cohort script are
--   presently hard-coded in both the loyalty cohort script and the PCORnetLoader script
-- 3) Finally, be sure you have run the meds schemachange script on your medications ontology to create the additional columns.

-- 4) For testing, change the 100000000 number to something small, like 10000.
-- 5) Change the 6 to something smaller to filter out fewer patients with low fact counts
-- 6) Run this from the database with the PopMedNet transforms and tables.   
--    Note that it could take a long time to run. (Should take ~30min per 10k patients, so about 1 day per 500k patients.)
--    The transform runs each procedure individually, to give the administrator finer-grained control. 
--    
-- All data from 1-1-2010 is transformed.
-- Jeff Klann, PhD


exec PCORnetPrep 6,100000000
GO

exec pcornetclear
GO
delete from provider
GO
exec PCORNETProvider
GO
delete from pmnharvest
GO
exec PCORNetHarvest
GO
delete from pmndemographic
GO
exec PCORNetDemographics
GO
delete from pmnENCOUNTER
GO
exec PCORNetEncounter
GO
delete from pmndiagnosis
GO
exec PCORNetDiagnosis
GO
delete from pmncondition
GO
exec PCORNetCondition
GO
delete from pmnprocedures
GO
exec PCORNetProcedure
GO
delete from pmnvital
GO
exec PCORNetVital
GO
delete from PMNenrollment
GO
exec PCORNetEnroll
GO
delete from pmnlab_result_cm
GO
exec PCORNetLabResultCM
GO
delete from pmnprescribing
GO
exec PCORNetPrescribing
GO
delete from pmndispensing
GO
exec PCORNetDispensing
GO
delete from pmnDeath
GO
exec PCORNetDeath
GO
exec pcornetreport
GO

select * from i2pReport;


