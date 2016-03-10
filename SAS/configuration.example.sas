/*******************************************************************************
* Oracle database connection and SAS data step view library configuration.  
* 
* For use in generating and inspecting PCORnet CMDv3 SAS data step views. 
*
* Copy or rename this file to configuration.sas and replace the following 
* placeholders with locally relavent values before running any of the associated
* SAS scripts which include configuration.sas:
*  - ORC_SCHEMA
*  - ORC_USERNAME
*  - ORC_PASSWORD
*  - OCR_HOSTNAME
*  - ORC_SID
*  - VIEW_LIB_PATH (e.x. C:/path/to/desired/data/step/view/directory/)
*
*******************************************************************************/


***************************************************************;
* Oracle database connection information
***************************************************************;
libname oracdata oracle schema=ORC_SCHEMA user=ORC_USERNAME pw=ORC_PASSWORD 
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


***************************************************************;
* Data step view location
***************************************************************; 
libname sasdata 'VIEW_LIB_PATH';
