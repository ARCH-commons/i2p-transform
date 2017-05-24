# Change Log

## [Unreleased](https://github.com/SCILHS/i2p-transform/tree/HEAD)

[Full Changelog](https://github.com/SCILHS/i2p-transform/compare/v0.6.1-beta...HEAD)

**Implemented enhancements:**

- Implement run script [\#40](https://github.com/SCILHS/i2p-transform/issues/40)
- New indexes! [\#38](https://github.com/SCILHS/i2p-transform/issues/38)
- Dispensing transform: bad default for unknown NDC codes [\#37](https://github.com/SCILHS/i2p-transform/issues/37)
- Apply Nathan's join change to SQLServer [\#31](https://github.com/SCILHS/i2p-transform/issues/31)
- Synonyms for pmnxxx-\>xxx [\#27](https://github.com/SCILHS/i2p-transform/issues/27)
- px\_date is not filled in in pmnprocedure [\#16](https://github.com/SCILHS/i2p-transform/issues/16)
- Add example of specifying password in SAS libname [\#15](https://github.com/SCILHS/i2p-transform/issues/15)
- Instance num is not supported [\#11](https://github.com/SCILHS/i2p-transform/issues/11)
- only patients active since 2010? [\#4](https://github.com/SCILHS/i2p-transform/issues/4)
- support DEATH table? [\#3](https://github.com/SCILHS/i2p-transform/issues/3)
- support BIOBANK\_FLAG facts? [\#2](https://github.com/SCILHS/i2p-transform/issues/2)

**Fixed bugs:**

- Custom mappings in lab transform: need to change 'LOINC:' to their local term [\#47](https://github.com/SCILHS/i2p-transform/issues/47)
- how is the PRESCRIBING pivot modifier\_cd constraint supposed to work? [\#46](https://github.com/SCILHS/i2p-transform/issues/46)
- Lab RESULT\_QUAL should be mapped to OT for this round [\#44](https://github.com/SCILHS/i2p-transform/issues/44)
- Griffin's ICD9 duplicates problem [\#43](https://github.com/SCILHS/i2p-transform/issues/43)
- Lab transform: RAW\_RESULT causes truncation errors [\#41](https://github.com/SCILHS/i2p-transform/issues/41)
- Lab transform does not account for local children [\#39](https://github.com/SCILHS/i2p-transform/issues/39)
- Dispensing transform: bad default for unknown NDC codes [\#37](https://github.com/SCILHS/i2p-transform/issues/37)
- Procedure replication error [\#36](https://github.com/SCILHS/i2p-transform/issues/36)
- RESULT\_DATE in labs cannot be null [\#34](https://github.com/SCILHS/i2p-transform/issues/34)
- Diagnosis: does not strip prefix [\#33](https://github.com/SCILHS/i2p-transform/issues/33)
- Apply Nathan's join change to SQLServer [\#31](https://github.com/SCILHS/i2p-transform/issues/31)
- Diagnosis: does not strip prefix [\#28](https://github.com/SCILHS/i2p-transform/issues/28)
- Synonyms for pmnxxx-\\>xxx [\#27](https://github.com/SCILHS/i2p-transform/issues/27)
- CPT F and T codes [\#26](https://github.com/SCILHS/i2p-transform/issues/26)
- Centimeters to inches unit conversion for heights [\#23](https://github.com/SCILHS/i2p-transform/issues/23)
- Synonym bug and truncation bug in pmnprescribing [\#21](https://github.com/SCILHS/i2p-transform/issues/21)
- Building the prescribing table in Oracle exhausts TEMP space [\#20](https://github.com/SCILHS/i2p-transform/issues/20)
- Harvest table fields require leading zero [\#18](https://github.com/SCILHS/i2p-transform/issues/18)
- PX\_SOURCE needs a default value  [\#17](https://github.com/SCILHS/i2p-transform/issues/17)
- px\\_date is not filled in in pmnprocedure [\#16](https://github.com/SCILHS/i2p-transform/issues/16)
- \*\_TIME fields are using 12 hour clock not 24 hour clock [\#13](https://github.com/SCILHS/i2p-transform/issues/13)
- Vitals does not use modifier\_cd for position [\#12](https://github.com/SCILHS/i2p-transform/issues/12)

**Closed issues:**

- PCORnet V3 table names [\#50](https://github.com/SCILHS/i2p-transform/issues/50)
- Griffin's LabResult\_CM procedure issue with 'LOINC:' term [\#49](https://github.com/SCILHS/i2p-transform/issues/49)
- Griffin's Smoking status character limit issue [\#48](https://github.com/SCILHS/i2p-transform/issues/48)
- Monolithic transform file is inconvenient for building tables individually or in parallel [\#32](https://github.com/SCILHS/i2p-transform/issues/32)
- RX\_FREQUENCY:02 vs 02 in pcori\_basecode [\#25](https://github.com/SCILHS/i2p-transform/issues/25)
- Add COMMIT to Oracle procedures... [\#22](https://github.com/SCILHS/i2p-transform/issues/22)
- vital.ht units [\#5](https://github.com/SCILHS/i2p-transform/issues/5)

## [v0.6.1-beta](https://github.com/SCILHS/i2p-transform/tree/v0.6.1-beta) (2015-12-16)
[Full Changelog](https://github.com/SCILHS/i2p-transform/compare/Version 5 for MSSQL...v0.6.1-beta)

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


\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*