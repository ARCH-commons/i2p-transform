/* Post-processing tasks against the CDM
*/

/* Copy providerid from encounter table to diagnosis, procedures tables.
CDM specification says:
  "Please note: This is a field replicated from the ENCOUNTER table."
*/
merge into diagnosis d
using encounter e
   on (d.encounterid = e.encounterid)
when matched then update set d.providerid = e.providerid;

merge into procedures p
using encounter e
   on (p.encounterid = e.encounterid)
when matched then update set p.providerid = e.providerid;

merge into prescribing p
using encounter e
   on (p.encounterid = e.encounterid)
when matched then update set p.rx_providerid = e.providerid;

/* Currently in HERON, systolic and diastolic blood pressures from flowsheets 
are reversed.  This is fixed in the HERON ETL code (#4026) but we haven't run a 
production ETL yet. 
*/
update vital v set v.systolic = v.diastolic, v.diastolic = v.systolic;

/* Currently in HERON, we have hight in cm and weight in kg - the CDM wants
height in inches and weight in pounds. */
update vital v set v.ht = v.ht / 2.54;
update vital v set v.wt = v.wt * 2.20462;
