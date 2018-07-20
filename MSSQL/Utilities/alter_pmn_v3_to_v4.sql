--Run this against a PCORNet CDM v. 3.0 MSSQL database in order to make the tables compliant with CDM v. 4.0

Alter Table pmndemographic add PAT_PREF_LANGUAGE_SPOKEN  varchar(3) NULL;

Alter Table pmnencounter add PAYER_TYPE_PRIMARY varchar(5) NULL;

Alter Table pmnencounter add PAYER_TYPE_SECONDARY varchar(5) NULL;

Alter Table pmnencounter add FACILITY_TYPE varchar(50) NULL;

Alter Table pmnpro_cm add  PRO_TYPE varchar(2) NULL;

Alter Table pmnpro_cm add  PRO_ITEM_LOINC varchar(10) NULL;

Alter Table pmnpro_cm add  PRO_RESPONSE_TEXT varchar(50) NULL;

Alter Table pmnpro_cm add PRO_ITEM_NAME varchar(50) NULL;

Alter Table pmnpro_cm add PRO_ITEM_VERSION varchar(50) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_NAME varchar(50) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_SEQ varchar(50) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_SCORE numeric(15,8) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_THETA numeric(15,8) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_SCALED_TSCORE numeric(15,8) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_STANDARD_ERROR numeric(15,8) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_COUNT_SCORED numeric(15,8) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_LOINC varchar(10) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_VERSION varchar(50) NULL;

Alter Table pmnpro_cm add PRO_ITEM_FULLNAME varchar(50) NULL;

Alter Table pmnpro_cm add PRO_ITEM_TEXT varchar(50) NULL;

Alter Table pmnpro_cm add PRO_MEASURE_FULLNAME varchar(50) NULL;

Alter Table pmnpro_cm add PRO_RESPONSE_NUM numeric(15,8) NULL;

Alter Table pmnlab_result_cm add RESULT_SNOMED varchar(50) NULL;

Alter Table pmnprescribing add RX_DOSE_ORDERED numeric(15, 8) NULL;
    
Alter Table pmnprescribing add RX_DOSE_ORDERED_UNIT varchar(50) NULL;

Alter Table pmnprescribing add RX_ROUTE varchar(50) NULL;

Alter Table pmnprescribing add RX_SOURCE varchar(2) NULL;
    
Alter Table pmnprescribing add RX_DISPENSE_AS_WRITTEN varchar(2) NULL;

Alter Table pmnprescribing add RX_PRN_FLAG varchar(1) NULL;

Alter Table pmnprescribing add RX_DOSE_FORM varchar(50) NULL;

Alter Table pmndiagnosis add DX_POA varchar(2) NULL;

Alter Table pmndispensing add  DISPENSE_DOSE_DISP numeric(15, 8) NULL;
   
Alter Table pmndispensing add DISPENSE_DOSE_DISP_UNIT varchar(50) NULL;

Alter Table pmndispensing add DISPENSE_ROUTE varchar(50) NULL;

Alter Table pmnprocedures add PPX varchar(2) NULL;

------------------------------RAW FIELDS----------------------------------------

Alter Table pmndemographic add RAW_PAT_PREF_LANGUAGE_SPOKEN  varchar(50) NULL;

Alter Table pmnencounter add RAW_FACILITY_TYPE  varchar(50) NULL;

Alter Table pmnencounter add RAW_PAYER_TYPE_PRIMARY varchar(50) NULL;

Alter Table pmnencounter add RAW_PAYER_NAME_PRIMARY  varchar(50) NULL;

Alter Table pmnencounter add RAW_PAYER_ID_PRIMARY  varchar(50) NULL;

Alter Table pmnencounter add RAW_PAYER_TYPE_SECONDARY  varchar(50) NULL;

Alter Table pmnencounter add RAW_PAYER_NAME_SECONDARY  varchar(50) NULL;

Alter Table pmnencounter add RAW_PAYER_ID_SECONDARY  varchar(50) NULL;

Alter Table pmndiagnosis add RAW_DX_POA  varchar(50) NULL;

Alter Table pmnprescribing add RAW_RX_DOSE_ORDERED varchar(50) NULL;

Alter Table pmnprescribing add RAW_RX_DOSE_ORDERED_UNIT varchar(50) NULL;

Alter Table pmnprescribing add RAW_RX_ROUTE varchar(50) NULL;

Alter Table pmnprescribing add RAW_RX_REFILLS varchar(50) NULL;

Alter Table pmndiagnosis add RAW_DX_POA varchar(50) NULL;

Alter Table pmndispensing add RAW_DISPENSE_DOSE_DISP varchar(50) NULL;

Alter Table pmndispensing add RAW_DISPENSE_DOSE_DISP_UNIT varchar(50) NULL;

Alter Table pmndispensing add RAW_DISPENSE_ROUTE varchar(50) NULL;

Alter Table pmnprocedures add RAW_PPX varchar(50) NULL;

------------------------------HARVEST----------------------------------------

Alter Table pmnharvest add DEATH_DATE_MGMT varchar(2) NULL;

Alter Table pmnharvest add MEDADMIN_START_DATE_MGMT varchar(2) NULL;

Alter Table pmnharvest add MEDADMIN_STOP_DATE_MGMT varchar(2) NULL;

Alter Table pmnharvest add OBSCLIN_DATE_MGMT varchar(2) NULL;

Alter Table pmnharvest add OBSGEN_DATE_MGMT varchar(2) NULL;

Alter Table pmnharvest add REFRESH_MED_ADMIN_DATE datetime NULL;

Alter Table pmnharvest add REFRESH_OBS_CLIN_DATE datetime NULL;

Alter Table pmnharvest add REFRESH_OBS_GEN_DATE datetime NULL;

Alter Table pmnharvest add REFRESH_PROVIDER_DATE datetime NULL; 
