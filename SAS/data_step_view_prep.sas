/*******************************************************************************
* Generate the data step views required by the PRCOnet Diagnostic Query 
* (diagnostic.sas).
*
* Provided the following values below before execution:
*  - ORC_SCHEMA
*  - ORC_USERNAME
*  - ORC_PASSWORD
*  - OCR_HOSTNAME
*  - ORC_SID
*  - VIEW_LIB_PATH
*
*******************************************************************************/

ODS HTML CLOSE;
ODS HTML;

libname oracdata oracle schema=ORC_SCEMA user=ORC_USERNAME pw=ORC_PASSWORD 
  path="(DESCRIPTION = 
    (ADDRESS = 
      (PROTOCOL = TCP)
      (HOST = ORC_HOSTNAME)
      (PORT = 1521)
    )
    (CONNECT_DATA = 
      (SID = ORC_SID)
    )
  )";
  
libname sasdata 'VIEW_LIB_PATH';


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

	BIRTH_TIME = input(_BIRTH_TIME, hhmmss.);
	format BIRTH_TIME hhmm.;
	drop _BIRTH_TIME;
run;


***************************************************************;
* Create data step view for ENROLLMENT
***************************************************************;

data sasdata.ENROLLMENT / view=sasdata.ENROLLMENT;
	set oracdata.ENROLLMENT;
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

	ADMIT_TIME = input(_ADMIT_TIME, hhmmss.);
	format ADMIT_TIME hhmm.;
	drop _ADMIT_TIME;

	DISCHARGE_TIME = input(_DISCHARGE_TIME, hhmmss.);
	format DISCHARGE_TIME hhmm.;
	drop _DISCHARGE_TIME;
run;


***************************************************************;
* Create data step view for DIAGNOSIS
***************************************************************;

data sasdata.DIAGNOSIS / view=sasdata.DIAGNOSIS;
	set oracdata.DIAGNOSIS;
run;


***************************************************************;
* Create data step view for PROCEDURES
***************************************************************;

data sasdata.PROCEDURES / view=sasdata.PROCEDURES;
	set oracdata.PROCEDURES;
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

	MEASURE_TIME = input(_MEASURE_TIME, hhmmss.);
	format MEASURE_TIME hhmm.;
	drop _MEASURE_TIME;
run;


***************************************************************;
* Create data step view for DISPENSING
***************************************************************;

data sasdata.DISPENSING / view=sasdata.DISPENSING;
	set oracdata.DISPENSING;
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

	RESULT_TIME = input(_RESULT_TIME, hhmmss.);
	format RESULT_TIME hhmm.;
	drop _RESULT_TIME;
	
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

	RX_ORDER_TIME = input(_RX_ORDER_TIME, hhmmss.);
	format RX_ORDER_TIME hhmm.;
	drop _RX_ORDER_TIME;
run;


***************************************************************;
* Create data step view for PCORNET_TRIAL
***************************************************************;

data sasdata.PCORNET_TRIAL / view=sasdata.PCORNET_TRIAL;
	set oracdata.PCORNET_TRIAL;
run;


***************************************************************;
* Create data step view for DEATH
***************************************************************;

data sasdata.DEATH / view=sasdata.DEATH;
	set oracdata.DEATH;
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
run;
