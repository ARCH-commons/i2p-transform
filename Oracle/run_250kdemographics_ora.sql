-- Simple script to execute pcornet loader on a subset of patients -oracle version
-- Alter the views to match your database name, and make more complex if you like.
-- Currently picks the first 250k patients for demographics transform and the first 10k matching encounters.
-- Jeff Klann 10/20/14 - Oracle version

drop synonym i2b2patient
/
drop view i2b2patient
/
-- Change to match your database name
create view i2b2patient as select * from (select * from patient_dimension) where rownum<250000
/
drop synonym i2b2visit
/
drop view i2b2visit
/
-- Change to match your database name
create view i2b2visit as select * from (select * from visit_dimension where patient_num in (select patient_num from i2b2patient)) where rownum<10000
/
begin
 pcornetloader;
end;
/
