CREATE TABLE pmnPROCEDURE(
	PATID varchar(50),
	ENCOUNTERID varchar(50),
	ENC_TYPE varchar(2),
	ADMIT_DATE varchar(10),
	PROVIDERID varchar(50),
	PX varchar(11),
	PX_TYPE varchar(2),
	RAW_PX varchar(50),
	RAW_PX_TYPE varchar(50) NULL
)
/
CREATE TABLE pmnVITAL(
	PATID varchar(50),
	ENCOUNTERID varchar(50),
	MEASURE_DATE varchar(10),
	MEASURE_TIME varchar(5),
	VITAL_SOURCE varchar(2),
	WT number(8, 0),
	DIASTOLIC number(4, 0),
	SYSTOLIC number(4, 0),
	BP_POSITION varchar(2),
	RAW_VITAL_SOURCE varchar(50),
	RAW_HT varchar(50),
	RAW_WT varchar(50),
	RAW_DIASTOLIC varchar(50),
	RAW_SYSTOLIC varchar(50),
	RAW_BP_POSITION varchar(50) NULL)
/
CREATE TABLE pmndiagnosis(
	PATID varchar(50),
	ENCOUNTERID varchar(50),
	ENC_TYPE varchar(2),
	ADMIT_DATE varchar(10),
	PROVIDERID varchar(50),
	DX varchar(18),
	DX_TYPE varchar(2),
	DX_SOURCE varchar(2),
	PDX varchar(2),
	RAW_DX varchar(50),
	RAW_DX_TYPE varchar(50),
	RAW_DX_SOURCE varchar(50),
	RAW_ORIGDX varchar(50),
	RAW_PDX varchar(50) NULL)
/
CREATE TABLE pmnENCOUNTER(
	PATID varchar(50),
	ENCOUNTERID varchar(50) NOT NULL,
	ADMIT_DATE varchar(10),
	ADMIT_TIME varchar(5),
	DISCHARGE_DATE varchar(10),
	DISCHARGE_TIME varchar(5),
	PROVIDERID varchar(50),
	FACILITY_LOCATION varchar(3),
	ENC_TYPE varchar(2),
	FACILITYID varchar(50),
	DISCHARGE_DISPOSITION varchar(2),
	DISCHARGE_STATUS varchar(2),
	DRG varchar(3),
	DRG_TYPE varchar(2),
	ADMITTING_SOURCE varchar(2),
	RAW_ENC_TYPE varchar(50),
	RAW_DISCHARGE_DISPOSITION varchar(50),
	RAW_DISCHARGE_STATUS varchar(50),
	RAW_DRG_TYPE varchar(50),
	RAW_ADMITTING_SOURCE varchar(50) )
/	
CREATE TABLE pmnENROLLMENT(
	PATID varchar(50),
	ENR_START_DATE varchar(10),
	ENR_END_DATE varchar(10),
	CHART varchar(1),
	BASIS varchar(1),
	RAW_CHART varchar(50),
	RAW_BASIS varchar(50))
/

CREATE TABLE pmndemographic(
	PATID varchar(50) NOT NULL,
	BIRTH_DATE varchar(10),
	BIRTH_TIME varchar(5),
	SEX varchar(2),
	HISPANIC varchar(2),
	RACE varchar(2),
	RAW_SEX varchar(50),
	RAW_HISPANIC varchar(50),
	RAW_RACE varchar(50))
/
