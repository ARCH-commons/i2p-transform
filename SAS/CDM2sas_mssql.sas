/*--------------------------------------------------------------------------------------\

Developed by Pedro Rivera, ADVANCE CDRN
Contact information: Pedro Rivera (riverap@ochin.org), Jon Puro (puroj@ochin.org)
Shared on an "as is" basis without warranties or conditions of any kind.
Developed with SAS 9.4 for use with SQL Server 2012.

Purpose: This script creates SAS data files from SQL Server tables for the PCORnet CDM v. 3.0.

----

Adapted for SCILHS by Jeff Klann, PhD
This version is also for SQLServer.
Changes: ID translation, altered date/time translation, altered tablenames, reporting code at end.

Instructions:

1. In Windows, open ODBC DataSources and create a SQL Server ODBC Data Source for the PopMedNet db. Call it "PopMedNet".
2. Change the second libname line below to point to a place where you would like to store your SAS datamart.
3. Run the script.
4. Verify the numbers printed at the completion match the destval numbers in the i2preport. (i.e. that everything copied without error)
\--------------------------------------------------------------------------------------*/

libname sql_cdm odbc datasrc='PopMedNet'; 
/* optionally add a password above: e.g., libname sql_cdm odbc datasrc='ORACLE_PMN' password=myPassWord; */
libname cdm "C:\Users\maj60\Documents\CDM_june_refresh";


proc sql noprint; 

create table cdm.DEMOGRAPHIC (compress=yes) as

SELECT 
	PATID ,
	DATEPART(BIRTH_DATE) as BIRTH_DATE format date9., /* The ODBC connection brings this over as a SAS datetime, so conversion isn't necessary - I just strip the times. (jgk) */
		/* Oracle might require something more like INPUT( ADMIT_DATE , e8601da.) AS ADMIT_DATE format date9., */
	INPUT( BIRTH_TIME , time10.) AS BIRTH_TIME format hhmm.,
    SEX ,
	SEXUAL_ORIENTATION,
	GENDER_IDENTITY,
    HISPANIC ,
    RACE ,
    BIOBANK_FLAG ,
    RAW_SEX ,
	RAW_SEXUAL_ORIENTATION,
	RAW_GENDER_IDENTITY,
    RAW_HISPANIC ,
    RAW_RACE 
FROM SQL_CDM.PMNDEMOGRAPHIC
;


proc sql noprint; 

create table cdm.ENCOUNTER (compress=yes) as

SELECT ENCOUNTERID ,
       PATID ,
	   DATEPART( ADMIT_DATE) AS ADMIT_DATE format date9.,
	   INPUT( ADMIT_TIME , time10.) AS ADMIT_TIME format hhmm.,
	   DATEPART( DISCHARGE_DATE) AS DISCHARGE_DATE format date9.,
	   INPUT( DISCHARGE_TIME , time10.) AS DISCHARGE_TIME format hhmm.,
       PROVIDERID ,
       FACILITY_LOCATION ,
       ENC_TYPE ,
       FACILITYID ,
       DISCHARGE_DISPOSITION ,
       DISCHARGE_STATUS ,
       DRG ,
       DRG_TYPE ,
       ADMITTING_SOURCE ,
	   RAW_SITEID ,
       RAW_ENC_TYPE ,
       RAW_DISCHARGE_DISPOSITION ,
       RAW_DISCHARGE_STATUS ,
       RAW_DRG_TYPE ,
       RAW_ADMITTING_SOURCE 
FROM SQL_CDM.PMNENCOUNTER
;



proc sql noprint; 

create table cdm.DIAGNOSIS (compress=yes) as

SELECT PUT(DIAGNOSISID,32.) AS DIAGNOSISID ,
       PATID ,
       ENCOUNTERID ,
       ENC_TYPE ,
       DATEPART( ADMIT_DATE ) AS ADMIT_DATE format date9.,
       PROVIDERID ,
       DX ,
       DX_TYPE ,
       DX_SOURCE ,
	   DX_ORIGIN ,
       PDX ,
       RAW_DX ,
       RAW_DX_TYPE ,
       RAW_DX_SOURCE ,
       RAW_PDX 
FROM SQL_CDM.PMNDIAGNOSIS
;

proc sql noprint; 

create table cdm.PROCEDURES (compress=yes) as

SELECT PUT(PROCEDURESID,32.) AS PROCEDURESID ,
       PATID ,
       ENCOUNTERID ,
       ENC_TYPE ,
       DATEPART( ADMIT_DATE ) AS ADMIT_DATE format date9.,
       PROVIDERID ,
	   DATEPART( PX_DATE ) AS PX_DATE format date9.,
       PX ,
       PX_TYPE ,
       PX_SOURCE ,
       RAW_PX ,
       RAW_PX_TYPE 
FROM SQL_CDM.PMNPROCEDURE
;


proc sql noprint; 

create table cdm.PRO_CM (compress=yes) as

SELECT PUT(PRO_CM_ID,32.) AS PRO_CM_ID,
       PATID ,
       ENCOUNTERID ,
       PRO_ITEM ,
       PRO_LOINC ,
	   DATEPART( PRO_DATE ) AS PRO_DATE format date9.,
	   INPUT( PRO_TIME , time10.) AS PRO_TIME format hhmm.,
       PRO_RESPONSE ,
       PRO_METHOD ,
       PRO_MODE ,
       PRO_CAT ,
       RAW_PRO_CODE ,
       RAW_PRO_RESPONSE 
FROM SQL_CDM.PMNPRO_CM
;

proc sql noprint; 

create table cdm.PRESCRIBING (compress=yes) as

SELECT PUT(PRESCRIBINGID,32.) AS PRESCRIBINGID,
       PATID ,
       ENCOUNTERID ,
       RX_PROVIDERID ,
	   DATEPART( RX_ORDER_DATE ) AS RX_ORDER_DATE format date9.,
	   INPUT( RX_ORDER_TIME , time10.) AS RX_ORDER_TIME format hhmm.,
	   DATEPART( RX_START_DATE ) AS RX_START_DATE format date9.,
	   DATEPART( RX_END_DATE ) AS RX_END_DATE format date9.,
       RX_QUANTITY ,
	   RX_QUANTITY_UNIT,
       RX_REFILLS ,
       RX_DAYS_SUPPLY ,
       RX_FREQUENCY ,
       RX_BASIS ,
       RXNORM_CUI ,
       RAW_RX_FREQUENCY ,
       RAW_RX_MED_NAME ,
       RAW_RXNORM_CUI
FROM SQL_CDM.PMNPRESCRIBING
;

proc sql noprint; 

create table cdm.DISPENSING (compress=yes) as

SELECT PUT(DISPENSINGID,32.) AS DISPENSINGID,
       PATID ,
       PUT(PRESCRIBINGID,32.) AS PRESCRIBINGID,
	   DATEPART( DISPENSE_DATE ) AS DISPENSE_DATE format date9.,
       NDC ,
       DISPENSE_SUP ,
       DISPENSE_AMT ,
       RAW_NDC 
FROM SQL_CDM.PMNDISPENSING
;


proc sql noprint; 

create table cdm.VITAL (compress=yes) as

SELECT PUT(VITALID,32.) AS VITALID ,
       PATID ,
       ENCOUNTERID ,
	   DATEPART( MEASURE_DATE ) AS MEASURE_DATE format date9.,
	   INPUT( MEASURE_TIME , time10.) AS MEASURE_TIME format hhmm.,
       VITAL_SOURCE ,
       HT ,
       WT ,
       DIASTOLIC ,
       SYSTOLIC ,
       ORIGINAL_BMI ,
       BP_POSITION ,
       SMOKING ,
       TOBACCO ,
       TOBACCO_TYPE ,
       RAW_BP_POSITION ,
       RAW_SYSTOLIC ,
       RAW_DIASTOLIC ,
       RAW_SMOKING ,
       RAW_TOBACCO ,
       RAW_TOBACCO_TYPE
FROM SQL_CDM.PMNVITAL
;

proc sql noprint; 

create table cdm.LAB_RESULT_CM (compress=yes) as

SELECT 
	   PUT(LAB_RESULT_CM_ID,32.) AS LAB_RESULT_CM_ID ,
       PATID ,
       ENCOUNTERID ,
       LAB_NAME ,
       SPECIMEN_SOURCE ,
       LAB_LOINC ,
       PRIORITY ,
       RESULT_LOC ,
       LAB_PX ,
       LAB_PX_TYPE ,
	   DATEPART( LAB_ORDER_DATE ) AS LAB_ORDER_DATE format date9.,
	   DATEPART( SPECIMEN_DATE ) AS SPECIMEN_DATE format date9.,
	   INPUT( SPECIMEN_TIME , time10.) AS SPECIMEN_TIME format hhmm.,
	   DATEPART( RESULT_DATE ) AS RESULT_DATE format date9.,
	   INPUT( RESULT_TIME , time10.) AS RESULT_TIME format hhmm., 
       RESULT_QUAL ,
       RESULT_NUM , /* Update 3/15/16: We now leave this as a decimal; the December CDM errata state this is ok  */
       RESULT_MODIFIER ,
       RESULT_UNIT ,
       NORM_RANGE_LOW ,
       NORM_MODIFIER_LOW ,
       NORM_RANGE_HIGH ,
       NORM_MODIFIER_HIGH ,
       ABN_IND ,
       RAW_LAB_NAME ,
       RAW_LAB_CODE ,
       RAW_PANEL ,
       RAW_RESULT ,
       RAW_UNIT ,
       RAW_ORDER_DEPT ,
       RAW_FACILITY_CODE 
FROM SQL_CDM.PMNLABRESULTS_CM
;

proc sql noprint; 

create table cdm.ENROLLMENT (compress=yes) as

SELECT PATID ,
       DATEPART( ENR_START_DATE ) AS ENR_START_DATE format date9.,
       DATEPART( ENR_END_DATE ) AS ENR_END_DATE format date9.,  
       CHART ,
       ENR_BASIS /* Note in SCILHS this column was accidentally called just 'BASIS' until the 1/11/16 fix. */
FROM SQL_CDM.PMNENROLLMENT
;


proc sql noprint; 

create table cdm.CONDITION (compress=yes) as

SELECT PUT(CONDITIONID,32.) AS CONDITIONID,
       PATID ,
       ENCOUNTERID ,
	   DATEPART( REPORT_DATE ) AS REPORT_DATE format date9.,
	   DATEPART( RESOLVE_DATE ) AS RESOLVE_DATE format date9.,
       DATEPART( ONSET_DATE ) AS ONSET_DATE format date9.,
       CONDITION_STATUS ,
       CONDITION ,
       CONDITION_TYPE ,
       CONDITION_SOURCE ,
       RAW_CONDITION_STATUS ,
       RAW_CONDITION ,
       RAW_CONDITION_TYPE ,
       RAW_CONDITION_SOURCE 
FROM SQL_CDM.PMNCONDITION
;


proc sql noprint; 

create table cdm.PCORNET_TRIAL (compress=yes) as

SELECT PATID ,
       TRIALID , /* In SCILHS, named incorrectly until 1/16/15 */
       PARTICIPANTID ,
       TRIAL_SITEID ,
       DATEPART( TRIAL_ENROLL_DATE ) AS TRIAL_ENROLL_DATE format date9.,
       DATEPART( TRIAL_END_DATE ) AS TRIAL_END_DATE format date9.,
	   DATEPART( TRIAL_WITHDRAW_DATE ) AS TRIAL_WITHDRAW_DATE format date9.,
       TRIAL_INVITE_CODE
FROM SQL_CDM.PMNPCORNET_TRIAL
;


proc sql noprint; 

create table cdm.DEATH (compress=yes) as

SELECT PATID ,
       DATEPART( DEATH_DATE ) AS DEATH_DATE format date9.,
       DEATH_DATE_IMPUTE ,
       DEATH_SOURCE ,
       DEATH_MATCH_CONFIDENCE 
FROM SQL_CDM.PMNDEATH
;

proc sql noprint; 

create table cdm.DEATH_CAUSE (compress=yes) as

SELECT PATID ,
       DEATH_CAUSE ,
       DEATH_CAUSE_CODE ,
       DEATH_CAUSE_TYPE ,
       DEATH_CAUSE_SOURCE ,
       DEATH_CAUSE_CONFIDENCE 
FROM SQL_CDM.PMNDEATH_CAUSE
;



proc sql noprint; 

create table cdm.HARVEST (compress=yes) as

SELECT NETWORKID ,
       NETWORK_NAME ,
       DATAMARTID ,
       DATAMART_NAME ,
       DATAMART_PLATFORM ,
       CDM_VERSION ,
       DATAMART_CLAIMS ,
       DATAMART_EHR ,
       BIRTH_DATE_MGMT ,
       ENR_START_DATE_MGMT ,
       ENR_END_DATE_MGMT ,
       ADMIT_DATE_MGMT ,
       DISCHARGE_DATE_MGMT ,
       PX_DATE_MGMT ,
       RX_ORDER_DATE_MGMT ,
       RX_START_DATE_MGMT ,
       RX_END_DATE_MGMT ,
       DISPENSE_DATE_MGMT ,
       LAB_ORDER_DATE_MGMT ,
       SPECIMEN_DATE_MGMT ,
       RESULT_DATE_MGMT ,
       MEASURE_DATE_MGMT ,
       ONSET_DATE_MGMT ,
       REPORT_DATE_MGMT ,
       RESOLVE_DATE_MGMT ,
       PRO_DATE_MGMT ,
	   DATEPART( REFRESH_DEMOGRAPHIC_DATE ) AS REFRESH_DEMOGRAPHIC_DATE format date9.,
	   DATEPART( REFRESH_ENROLLMENT_DATE ) AS REFRESH_ENROLLMENT_DATE format date9.,
	   DATEPART( REFRESH_ENCOUNTER_DATE ) AS REFRESH_ENCOUNTER_DATE format date9.,
	   DATEPART( REFRESH_DIAGNOSIS_DATE ) AS REFRESH_DIAGNOSIS_DATE format date9.,
	   DATEPART( REFRESH_PROCEDURES_DATE ) AS REFRESH_PROCEDURES_DATE format date9.,
	   DATEPART( REFRESH_VITAL_DATE ) AS REFRESH_VITAL_DATE format date9.,
	   DATEPART( REFRESH_DISPENSING_DATE ) AS REFRESH_DISPENSING_DATE format date9.,
	   DATEPART( REFRESH_LAB_RESULT_CM_DATE ) AS REFRESH_LAB_RESULT_CM_DATE format date9.,
	   DATEPART( REFRESH_CONDITION_DATE ) AS REFRESH_CONDITION_DATE format date9.,
	   DATEPART( REFRESH_PRO_CM_DATE ) AS REFRESH_PRO_CM_DATE format date9.,
	   DATEPART( REFRESH_PRESCRIBING_DATE ) AS REFRESH_PRESCRIBING_DATE format date9.,
	   DATEPART( REFRESH_PCORNET_TRIAL_DATE ) AS REFRESH_PCORNET_TRIAL_DATE format date9.,
	   DATEPART( REFRESH_DEATH_DATE ) AS REFRESH_DEATH_DATE format date9.,
	   DATEPART( REFRESH_DEATH_CAUSE_DATE) AS REFRESH_DEATH_CAUSE_DATE format date9.
FROM SQL_CDM.PMNHARVEST
;

*-- Borrowed from the SAS documentation --*;
%macro obsnvars(ds);
   %global dset nvars nobs;
   %let dset=&ds;
   %let dsid = %sysfunc(open(&dset));
   %if &dsid %then
      %do;
         %let nobs =%sysfunc(attrn(&dsid,NOBS));
         *%let nvars=%sysfunc(attrn(&dsid,NVARS));
         %let rc = %sysfunc(close(&dsid));
         %put &dset - &nobs observation(s).;
      %end;
   %else
      %put Open for data set &dset failed - %sysfunc(sysmsg());
%mend obsnvars;

*-- Print the total counts --*;
options nosource;
%obsnvars(CDM.DEMOGRAPHIC);
%obsnvars(CDM.ENROLLMENT);
%obsnvars(CDM.ENCOUNTER);
%obsnvars(CDM.DIAGNOSIS);
%obsnvars(CDM.PROCEDURES);
%obsnvars(CDM.CONDITION);
%obsnvars(CDM.VITAL);
%obsnvars(CDM.LAB_RESULT_CM);
%obsnvars(CDM.PRESCRIBING);
%obsnvars(CDM.DISPENSING);
options source;

