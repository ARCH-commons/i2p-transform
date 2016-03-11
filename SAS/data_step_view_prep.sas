/*******************************************************************************
* Generate the PCORnet CDMv3 data step views required by PCORnet SAS queries, 
* providing the required data type transformations where needed. 
*******************************************************************************/


***************************************************************;
* Clear SAS result buffer
***************************************************************;
ODS HTML CLOSE;
ODS HTML;


***************************************************************;
* Include configurable SAS libraies
***************************************************************;
%let fpath=%sysget(SAS_EXECFILEPATH);
%let fname=%sysget(SAS_EXECFILENAME);
%let path= %sysfunc(tranwrd(&fpath,&fname,''));
%put &path;
%include '&path/configuration.sas';


***************************************************************;
* Create data step view for DEMOGRAPHIC
***************************************************************;
data sasdata.DEMOGRAPHIC / view=sasdata.DEMOGRAPHIC;
	set oracdata.DEMOGRAPHIC(
		rename = (
			BIRTH_TIME = _BIRTH_TIME
		)
	)
	;

	BIRTH_DATE = datepart(BIRTH_DATE);
    format BIRTH_DATE mmddyy10.;

	BIRTH_TIME = input(_BIRTH_TIME, hhmmss.);
	format BIRTH_TIME hhmm.;
	drop _BIRTH_TIME;
run;


***************************************************************;
* Create data step view for ENROLLMENT
***************************************************************;
data sasdata.ENROLLMENT / view=sasdata.ENROLLMENT;
	set oracdata.ENROLLMENT;

	ENR_START_DATE = datepart(ENR_START_DATE);
    format ENR_START_DATE mmddyy10.;

	ENR_END_DATE = datepart(ENR_END_DATE);
    format ENR_END_DATE mmddyy10.;

run;


***************************************************************;
* Create data step view for ENCOUNTER
***************************************************************;
data sasdata.ENCOUNTER / view=sasdata.ENCOUNTER;
	set oracdata.ENCOUNTER(
		rename = (
			ADMIT_TIME = _ADMIT_TIME
			DISCHARGE_TIME = _DISCHARGE_TIME
		)
	)
	;

	ADMIT_DATE = datepart(ADMIT_DATE);
    format ADMIT_DATE mmddyy10.;

	ADMIT_TIME = input(_ADMIT_TIME, hhmmss.);
	format ADMIT_TIME hhmm.;
	drop _ADMIT_TIME;

	DISCHARGE_DATE = datepart(DISCHARGE_DATE);
    format DISCHARGE_DATE mmddyy10.;

	DISCHARGE_TIME = input(_DISCHARGE_TIME, hhmmss.);
	format DISCHARGE_TIME hhmm.;
	drop _DISCHARGE_TIME;
run;


***************************************************************;
* Create data step view for DIAGNOSIS
***************************************************************;
data sasdata.DIAGNOSIS / view=sasdata.DIAGNOSIS;
	set oracdata.DIAGNOSIS;

	ADMIT_DATE = datepart(ADMIT_DATE);
    format ADMIT_DATE mmddyy10.;
run;


***************************************************************;
* Create data step view for PROCEDURES
***************************************************************;
data sasdata.PROCEDURES / view=sasdata.PROCEDURES;
	set oracdata.PROCEDURES;

	ADMIT_DATE = datepart(ADMIT_DATE);
    format ADMIT_DATE mmddyy10.;

	PX_DATE = datepart(PX_DATE);
    format PX_DATE mmddyy10.;
run;


***************************************************************;
* Create data step view for VITAL
***************************************************************;
data sasdata.VITAL / view=sasdata.VITAL;
	set oracdata.VITAL(
		rename = (
			MEASURE_TIME = _MEASURE_TIME
		)
	)
	;

	MEASURE_DATE = datepart(MEASURE_DATE);
    format MEASURE_DATE mmddyy10.;

	MEASURE_TIME = input(_MEASURE_TIME, hhmmss.);
	format MEASURE_TIME hhmm.;
	drop _MEASURE_TIME;
run;


***************************************************************;
* Create data step view for DISPENSING
***************************************************************;
data sasdata.DISPENSING / view=sasdata.DISPENSING;
	set oracdata.DISPENSING;

	DISPENSE_DATE = datepart(DISPENSE_DATE);
    format DISPENSE_DATE mmddyy10.;
run;


***************************************************************;
* Create data step view for LAB_RESULT_CM
***************************************************************;
data sasdata.LAB_RESULT_CM / view=sasdata.LAB_RESULT_CM;
	set oracdata.LAB_RESULT_CM(
		rename = (
			RESULT_TIME = _RESULT_TIME
			SPECIMEN_TIME = _SPECIMEN_TIME
			RESULT_NUM = _RESULT_NUM
		)
	)
	;
	
	LAB_ORDER_DATE = datepart(LAB_ORDER_DATE);
    format LAB_ORDER_DATE mmddyy10.;

	RESULT_DATE = datepart(RESULT_DATE);
    format RESULT_DATE mmddyy10.;

	RESULT_TIME = input(_RESULT_TIME, hhmmss.);
	format RESULT_TIME hhmm.;
	drop _RESULT_TIME;

	SPECIMEN_DATE = datepart(SPECIMEN_DATE);
    format SPECIMEN_DATE mmddyy10.;
	
	SPECIMEN_TIME = input(_SPECIMEN_TIME, hhmmss.);
	format SPECIMEN_TIME hhmm.;
	drop _SPECIMEN_TIME;
	
	RESULT_NUM = put(_RESULT_NUM, best8.);
	drop _RESULT_NUM;
run;


***************************************************************;
* Create data step view for CONDITION
***************************************************************;
data sasdata.CONDITION / view=sasdata.CONDITION;
	set oracdata.CONDITION;

	REPORT_DATE = datepart(REPORT_DATE);
    format REPORT_DATE mmddyy10.;

	RESOLVE_DATE = datepart(RESOLVE_DATE);
    format RESOLVE_DATE mmddyy10.;

	ONSET_DATE = datepart(ONSET_DATE);
    format ONSET_DATE mmddyy10.;
run;


***************************************************************;
* Create data step view for PRO_CM
***************************************************************;
data sasdata.PRO_CM / view=sasdata.PRO_CM;
	set oracdata.PRO_CM(
		rename = (
			PRO_TIME = _PRO_TIME
		)
	)
	;

	PRO_DATE = datepart(PRO_DATE);
    format PRO_DATE mmddyy10.;

	PRO_TIME = input(_PRO_TIME, hhmmss.);
	format PRO_TIME hhmm.;
	drop _PRO_TIME;
run;


***************************************************************;
* Create data step view for PRESCRIBING
***************************************************************;
data sasdata.PRESCRIBING / view=sasdata.PRESCRIBING;
	set oracdata.PRESCRIBING(
		rename = (
			RX_ORDER_TIME = _RX_ORDER_TIME
		)
	)
	;

	RX_ORDER_DATE = datepart(RX_ORDER_DATE);
    format RX_ORDER_DATE mmddyy10.;

	RX_ORDER_TIME = input(_RX_ORDER_TIME, hhmmss.);
	format RX_ORDER_TIME hhmm.;
	drop _RX_ORDER_TIME;

	RX_START_DATE = datepart(RX_START_DATE);
    format RX_START_DATE mmddyy10.;

	RX_END_DATE = datepart(RX_END_DATE);
    format RX_END_DATE mmddyy10.;
run;


***************************************************************;
* Create data step view for PCORNET_TRIAL
***************************************************************;
data sasdata.PCORNET_TRIAL / view=sasdata.PCORNET_TRIAL;
	set oracdata.PCORNET_TRIAL;

	TRIAL_ENROLL_DATE = datepart(TRIAL_ENROLL_DATE);
    format TRIAL_ENROLL_DATE mmddyy10.;

	TRIAL_END_DATE = datepart(TRIAL_END_DATE);
    format TRIAL_END_DATE mmddyy10.;

	TRIAL_WITHDRAW_DATE = datepart(TRIAL_WITHDRAW_DATE);
    format TRIAL_WITHDRAW_DATE mmddyy10.;
run;


***************************************************************;
* Create data step view for DEATH
***************************************************************;
data sasdata.DEATH / view=sasdata.DEATH;
	set oracdata.DEATH;

	DEATH_DATE = datepart(DEATH_DATE);
    format DEATH_DATE mmddyy10.;
run;


***************************************************************;
* Create data step view for DEATH_CAUSE
***************************************************************;
data sasdata.DEATH_CAUSE / view=sasdata.DEATH_CAUSE;
	set oracdata.DEATH_CAUSE;
run;


***************************************************************;
* Create data step view for HARVEST
***************************************************************;
data sasdata.HARVEST / view=sasdata.HARVEST;
	set oracdata.HARVEST;

	REFRESH_DEMOGRAPHIC_DATE = datepart(REFRESH_DEMOGRAPHIC_DATE);
    format REFRESH_DEMOGRAPHIC_DATE mmddyy10.;

	REFRESH_ENROLLMENT_DATE = datepart(REFRESH_ENROLLMENT_DATE);
    format REFRESH_ENROLLMENT_DATE mmddyy10.;

	REFRESH_ENCOUNTER_DATE = datepart(REFRESH_ENCOUNTER_DATE);
    format REFRESH_ENCOUNTER_DATE mmddyy10.;

	REFRESH_DIAGNOSIS_DATE = datepart(REFRESH_DIAGNOSIS_DATE);
    format REFRESH_DIAGNOSIS_DATE mmddyy10.;

	REFRESH_PROCEDURES_DATE = datepart(REFRESH_PROCEDURES_DATE);
    format REFRESH_PROCEDURES_DATE mmddyy10.;

	REFRESH_VITAL_DATE = datepart(REFRESH_VITAL_DATE);
    format REFRESH_VITAL_DATE mmddyy10.;

	REFRESH_DISPENSING_DATE = datepart(REFRESH_DISPENSING_DATE);
    format REFRESH_DISPENSING_DATE mmddyy10.;

	REFRESH_LAB_RESULT_CM_DATE = datepart(REFRESH_LAB_RESULT_CM_DATE);
    format REFRESH_LAB_RESULT_CM_DATE mmddyy10.;

	REFRESH_CONDITION_DATE = datepart(REFRESH_CONDITION_DATE);
    format REFRESH_CONDITION_DATE mmddyy10.;

	REFRESH_PRO_CM_DATE = datepart(REFRESH_PRO_CM_DATE);
    format REFRESH_PRO_CM_DATE mmddyy10.;

	REFRESH_PRESCRIBING_DATE = datepart(REFRESH_PRESCRIBING_DATE);
    format REFRESH_PRESCRIBING_DATE mmddyy10.;

	REFRESH_PCORNET_TRIAL_DATE = datepart(REFRESH_PCORNET_TRIAL_DATE);
    format REFRESH_PCORNET_TRIAL_DATE mmddyy10.;

	REFRESH_DEATH_DATE = datepart(REFRESH_DEATH_DATE);
    format REFRESH_DEATH_DATE mmddyy10.;

	REFRESH_DEATH_CAUSE_DATE = datepart(REFRESH_DEATH_CAUSE_DATE);
    format REFRESH_DEATH_CAUSE_DATE mmddyy10.;
run;
