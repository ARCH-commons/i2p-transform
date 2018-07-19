/*******************************************************************************
* Inspect the PCORnet CDMv3 data step views created by data_step_view_prep.sas. 
* Runs the SAS content procedure over and gets the first ten records from each 
* of the data step views.
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


proc contents data=sasdata.DEMOGRAPHIC;
run;
proc print data=sasdata.DEMOGRAPHIC (firstobs=1 obs=10);
run;


proc contents data=sasdata.ENROLLMENT;
run;
proc print data=sasdata.ENROLLMENT (firstobs=1 obs=10);
run;


proc contents data=sasdata.ENCOUNTER;
run;
proc print data=sasdata.ENCOUNTER (firstobs=1 obs=10);
run;


proc contents data=sasdata.DIAGNOSIS;
run;
proc print data=sasdata.DIAGNOSIS (firstobs=1 obs=10);
run;


proc contents data=sasdata.PROCEDURES;
run;
proc print data=sasdata.PROCEDURES (firstobs=1 obs=10);
run;


proc contents data=sasdata.VITAL;
run;
proc print data=sasdata.VITAL (firstobs=1 obs=10);
run;


proc contents data=sasdata.DISPENSING;
run;
proc print data=sasdata.DISPENSING (firstobs=1 obs=10);
run;


proc contents data=sasdata.LAB_RESULT_CM;
run;
proc print data=sasdata.LAB_RESULT_CM (firstobs=1 obs=10);
run;


proc contents data=sasdata.CONDITION;
run;
proc print data=sasdata.CONDITION (firstobs=1 obs=10);
run;


proc contents data=sasdata.PRO_CM;
run;
proc print data=sasdata.PRO_CM (firstobs=1 obs=10);
run;


proc contents data=sasdata.PRESCRIBING;
run;
proc print data=sasdata.PRESCRIBING (firstobs=1 obs=10);
run;


proc contents data=sasdata.PCORNET_TRIAL;
run;
proc print data=sasdata.PCORNET_TRIAL (firstobs=1 obs=10);
run;


proc contents data=sasdata.DEATH;
run;
proc print data=sasdata.DEATH (firstobs=1 obs=10);
run;


proc contents data=sasdata.DEATH_CAUSE;
run;
proc print data=sasdata.DEATH_CAUSE (firstobs=1 obs=10);
run;


proc contents data=sasdata.HARVEST;
run;
proc print data=sasdata.HARVEST (firstobs=1 obs=10);
run;
