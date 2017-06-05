# Old, manual changelog
---------
*10/8/14* - initial release of preparatory files

*10/20/14* - initial release of demographic transform

## Version 5 for MSSQL (12/16/14)

*12/16/14* - release of "version 5" for SQL Server, which transforms all sections of CDM v1.5 except for unit conversions and code translation on visit_dimension columns.  

- Version 5 by Jeff Klann, PhD; derived from version 4.1 and with contributions from Dan Connolly and Nathan Graham for vitals transformation
- Version 4.1 by Aaron Abend, aaronabend@gmail.com, 978-621-7745
- TODO: Code translation on encounter columns, unit conversion on vitals
- 5.0 Now transforms vitals, enrollment (encounter basis), encounters (new columns and DRGs), speed improvements in diagnosis, bugfixes in procedures
- 4.0 Splits HIB/HIW/etc codes into race and hispanic 
- 3.8 Removed Duplicates due to invalid codes introduced by use of isnull
- 3.6 Properly handle hispanic in the race_cd column
- 3.5 Properly handle null sex and null race 
- 3.4 fixed date formats in diagnosis, fixed extra/missing rows in diagnosis, removed unchecked drop statements
- 3.3 fixes diagnoses to not miss unmapped PDX code
- 3.2 fixes demographics - does not miss patients with unmapped sex or unmapped race
- 3.1. puts admit diagnosis modifier in dx_source of pmn

*3/19/15* - release of loyalty cohort transform script

## Version 5.1 for MSSQL (8/20/15)

*8/20/15* ­ release of "version 5.1" for SQL Server which transforms lab results.

## Version 5 for Oracle and 6 for MSSQL (10/15/15)

10/10/15 - release of "version 5" for Oracle, adapted by Wake Forest. Limited to CDM v1.5.2.

10/15/15 - release of "version 6" for SQL Server, which transforms:

- Vitals 2.0.2: Adds smoking and tobacco status
- Enrollment: Now adds a flag for loyalty cohort patients
- Labs v2.0.2
- Medications v2.0.2 into prescribing and dispensing tables
- Condition: Uses the CONDITION_SOURCE modifier in Diagnoses 2.0.2
- Various optimizations and bugfixes noted in the code, plus the following changes requested by the coordinating center:
 - Enrollment date ranges are now based on encounters not facts
 - Discharge disposition: forced to NI for AV
 - Discharge status: forced to NI on AV
 - Encounter columns like admit source now translate null to NI
 - dx_source forced to FI for AV
 - vital_source set to HC

## Version 6.1 for MSSQL and v6 for Oracle

*12/10/15* - release of "version 6.1" for SQL Server and "version 6" for Oracle:

- MSSQL and Oracle: Bug fixes in prescribing/dispensing - please re-run
- MSSQL: Speed optimizations in prescribing, dispensing, labs, condition, and diagnosis. Now each transform runs on 2m patients at Partners in <30m.
- Note that the Oracle version was adapted by colleagues at Wake Forest and has not been tested by SCILHS Central.

# Version 0.6.1 for MSSQL and Oracle (12/17/15)

- MSSQL and Oracle versions are once again equivalent. 
- Versioning schema changed from 6.x to 0.6.x
- Migrated to GitHub
- bugfix in tobbaco_type logic in vitals, 12/17/15

# Version 0.6.2 for Oracle (1/6/15)

- Minor bugs fixed in translation from MSSQL to Oracle

# Version 0.6.3 for MSSQL (1/13/15)

- Typos found in field names in trial, enrollment, vital, and harvest tables 
 - Enrollment:	Basis	-> Enr_basis
 - Vital:	Vitaliid	-> Vitalid
 - Harvest:	Speciment_date_mgmt	-> Specimen_date_mgmt
 - Pcornet_trial:	Trial_id ->	Trialid
- Wrong data type found in pmnprescribing (RX_FREQUENCY)
- tobbaco_type logic in vitals was faulty

# Version 0.6.5 for MSSQL

- Version 0.6.5, Harvest leading zero was still a bug!; wrong column referenced in pcornetprocedure; added simple (non-ontology) death transform; duplicate code in i2preport
- Version 0.6.4, Harvest leading zero bug, px_type default
