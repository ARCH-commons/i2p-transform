# SCILHS i2b2 to PCORnet Common Data Model (CDM) tables - SQL and SAS Transforms
### Changelog
### Jeff Klann, PhD

# Changelog
---------
*10/8/14* - initial release of preparatory files

*10/20/14* - initial release of demographic transform

## Version 5 for MSSQL (12/16/14)

*12/16/14* - release of "version 5" for SQL Server, which transforms all sections of CDM v1.5 except for unit conversions and code translation on visit_dimension columns.  

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
