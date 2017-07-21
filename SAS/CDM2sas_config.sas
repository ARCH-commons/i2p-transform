/*--------------------------------------------------------------------------------------\

Developed by Pedro Rivera, ADVANCE CDRN
Contact information: Pedro Rivera (riverap@ochin.org), Jon Puro (puroj@ochin.org)
Shared on an "as is" basis without warranties or conditions of any kind.
Adapted for SCILHS by Jeff Klann, PhD
Instructions:

1. In Windows, open ODBC DataSources and create a SQL Server ODBC Data Source for the PopMedNet db. Call it "PopMedNet".
2. Change the second libname line below to point to a place where you would like to store your SAS datamart.

\--------------------------------------------------------------------------------------*/

libname sql_cdm odbc datasrc='PopMedNet'; 
/* optionally add a password above: e.g., libname sql_cdm odbc datasrc='ORACLE_PMN' password=myPassWord; */
libname cdm "C:\Users\maj60\Documents\CDM_june_refresh";
