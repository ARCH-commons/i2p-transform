--------------------------------------------------------------------------------
-- PRESCRIBING
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE prescribing');
END;
/
CREATE TABLE prescribing(
	PRESCRIBINGID varchar(19)  primary key,
	PATID varchar(50) NOT NULL,
	ENCOUNTERID  varchar(50) NULL,
	RX_PROVIDERID varchar(50) NULL, -- NOTE: The spec has a _ before the ID, but this is inconsistent.
	RX_ORDER_DATE date NULL,
	RX_ORDER_TIME varchar (5) NULL,
	RX_START_DATE date NULL,
	RX_END_DATE date NULL,
	RX_QUANTITY number(18,5) NULL,
  RX_QUANTITY_UNIT varchar(2) NULL,
	RX_REFILLS number(18,5) NULL,
	RX_DAYS_SUPPLY number (18,5) NULL,
	RX_FREQUENCY varchar(2) NULL,
	RX_BASIS varchar (2) NULL,
	RXNORM_CUI varchar(8) NULL,
	RAW_RX_MED_NAME varchar (50) NULL,
	RAW_RX_FREQUENCY varchar (50) NULL,
  RAW_RX_QUANTITY varchar(50) NULL,
  RAW_RX_NDC varchar(50) NULL,
	RAW_RXNORM_CUI varchar (50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  prescribing_seq');
END;
/
create sequence  prescribing_seq
/

create or replace trigger prescribing_trg
before insert on prescribing
for each row
begin
  select prescribing_seq.nextval into :new.PRESCRIBINGID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE basis');
END;
/

CREATE TABLE BASIS  (
	PCORI_BASECODE	VARCHAR2(50) NULL,
	C_FULLNAME    	VARCHAR2(700) NOT NULL,
	INSTANCE_NUM  	NUMBER(18) NOT NULL,
	START_DATE    	DATE NOT NULL,
	PROVIDER_ID   	VARCHAR2(50) NOT NULL,
	CONCEPT_CD    	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM 	NUMBER(38) NOT NULL,
	MODIFIER_CD   	VARCHAR2(100) NOT NULL
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE freq');
END;
/

CREATE TABLE FREQ  (
	PCORI_BASECODE	VARCHAR2(50) NULL,
	INSTANCE_NUM  	NUMBER(18) NOT NULL,
	START_DATE    	DATE NOT NULL,
	PROVIDER_ID   	VARCHAR2(50) NOT NULL,
	CONCEPT_CD    	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM 	NUMBER(38) NOT NULL,
	MODIFIER_CD   	VARCHAR2(100) NOT NULL
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE quantity');
END;
/

CREATE TABLE QUANTITY  (
	NVAL_NUM     	NUMBER(18,5) NULL,
	INSTANCE_NUM 	NUMBER(18) NOT NULL,
	START_DATE   	DATE NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	MODIFIER_CD  	VARCHAR2(100) NOT NULL
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE refills');
END;
/

CREATE TABLE REFILLS  (
	NVAL_NUM     	NUMBER(18,5) NULL,
	INSTANCE_NUM 	NUMBER(18) NOT NULL,
	START_DATE   	DATE NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	MODIFIER_CD  	VARCHAR2(100) NOT NULL
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE supply');
END;
/

CREATE TABLE SUPPLY  (
	NVAL_NUM     	NUMBER(18,5) NULL,
	INSTANCE_NUM 	NUMBER(18) NOT NULL,
	START_DATE   	DATE NOT NULL,
	PROVIDER_ID  	VARCHAR2(50) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	MODIFIER_CD  	VARCHAR2(100) NOT NULL
	)
/
create or replace procedure PCORNetPrescribing as
begin

PMN_DROPSQL('drop index prescribing_idx');
PMN_DROPSQL('drop index basis_idx');
PMN_DROPSQL('drop index freq_idx');
PMN_DROPSQL('drop index quantity_idx');
PMN_DROPSQL('drop index refills_idx');
PMN_DROPSQL('drop index supply_idx');

execute immediate 'truncate table prescribing';
execute immediate 'truncate table basis';
execute immediate 'truncate table freq';
execute immediate 'truncate table quantity';
execute immediate 'truncate table refills';
execute immediate 'truncate table supply';

insert into basis
select pcori_basecode,c_fullname,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact basis
        inner join encounter enc on enc.patid = basis.patient_num and enc.encounterid = basis.encounter_Num
     join pcornet_med basiscode
        on basis.modifier_cd = basiscode.c_basecode
        and basiscode.c_fullname like '\PCORI_MOD\RX_BASIS\%';

execute immediate 'create unique index basis_idx on basis (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('BASIS');

insert into freq
select pcori_basecode,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact freq
        inner join encounter enc on enc.patid = freq.patient_num and enc.encounterid = freq.encounter_Num
     join pcornet_med freqcode
        on freq.modifier_cd = freqcode.c_basecode
        and freqcode.c_fullname like '\PCORI_MOD\RX_FREQUENCY\%';

execute immediate 'create unique index freq_idx on freq (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('FREQ');

insert into quantity
select nval_num,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact quantity
        inner join encounter enc on enc.patid = quantity.patient_num and enc.encounterid = quantity.encounter_Num
     join pcornet_med quantitycode
        on quantity.modifier_cd = quantitycode.c_basecode
        and quantitycode.c_fullname like '\PCORI_MOD\RX_QUANTITY\';

execute immediate 'create unique index quantity_idx on quantity (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('QUANTITY');

insert into refills
select nval_num,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact refills
        inner join encounter enc on enc.patid = refills.patient_num and enc.encounterid = refills.encounter_Num
     join pcornet_med refillscode
        on refills.modifier_cd = refillscode.c_basecode
        and refillscode.c_fullname like '\PCORI_MOD\RX_REFILLS\';

execute immediate 'create unique index refills_idx on refills (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('REFILLS');

insert into supply
select nval_num,instance_num,start_date,provider_id,concept_cd,encounter_num,modifier_cd from i2b2medfact supply
        inner join encounter enc on enc.patid = supply.patient_num and enc.encounterid = supply.encounter_Num
     join pcornet_med supplycode
        on supply.modifier_cd = supplycode.c_basecode
        and supplycode.c_fullname like '\PCORI_MOD\RX_DAYS_SUPPLY\';

execute immediate 'create unique index supply_idx on supply (instance_num, start_date, provider_id, concept_cd, encounter_num, modifier_cd)';
GATHER_TABLE_STATS('SUPPLY');

-- insert data with outer joins to ensure all records are included even if some data elements are missing
insert into prescribing (
	PATID
    ,encounterid
    ,RX_PROVIDERID
	,RX_ORDER_DATE -- using start_date from i2b2
	,RX_ORDER_TIME  -- using time start_date from i2b2
	,RX_START_DATE
	,RX_END_DATE
    ,RXNORM_CUI --using pcornet_med pcori_cui - new column!
    ,RX_QUANTITY ---- modifier nval_num
    ,RX_QUANTITY_UNIT
    ,RX_REFILLS  -- modifier nval_num
    ,RX_DAYS_SUPPLY -- modifier nval_num
    ,RX_FREQUENCY --modifier with basecode lookup
    ,RX_BASIS --modifier with basecode lookup
    ,RAW_RX_MED_NAME
--    ,RAW_RX_FREQUENCY,
    ,RAW_RXNORM_CUI
)
select distinct  m.patient_num, m.Encounter_Num,m.provider_id,  m.start_date order_date,  to_char(m.start_date,'HH24:MI'), m.start_date start_date, m.end_date, mo.pcori_cui
    ,quantity.nval_num quantity, 'NI' rx_quantity_unit, refills.nval_num refills, supply.nval_num supply, substr(freq.pcori_basecode, instr(freq.pcori_basecode, ':') + 1, 2) frequency,
    substr(basis.pcori_basecode, instr(basis.pcori_basecode, ':') + 1, 2) basis
    , substr(mo.c_name, 1, 50) raw_rx_med_name, substr(mo.c_basecode, 1, 50) raw_rxnorm_cui
 from i2b2medfact m inner join pcornet_med mo on m.concept_cd = mo.c_basecode
inner join encounter enc on enc.encounterid = m.encounter_Num
-- TODO: This join adds several minutes to the load - must be debugged

    left join basis
    on m.encounter_num = basis.encounter_num
    and m.concept_cd = basis.concept_Cd
    and m.start_date = basis.start_date
    and m.provider_id = basis.provider_id
    and m.instance_num = basis.instance_num

    left join  freq
    on m.encounter_num = freq.encounter_num
    and m.concept_cd = freq.concept_Cd
    and m.start_date = freq.start_date
    and m.provider_id = freq.provider_id
    and m.instance_num = freq.instance_num

    left join quantity
    on m.encounter_num = quantity.encounter_num
    and m.concept_cd = quantity.concept_Cd
    and m.start_date = quantity.start_date
    and m.provider_id = quantity.provider_id
    and m.instance_num = quantity.instance_num

    left join refills
    on m.encounter_num = refills.encounter_num
    and m.concept_cd = refills.concept_Cd
    and m.start_date = refills.start_date
    and m.provider_id = refills.provider_id
    and m.instance_num = refills.instance_num

    left join supply
    on m.encounter_num = supply.encounter_num
    and m.concept_cd = supply.concept_Cd
    and m.start_date = supply.start_date
    and m.provider_id = supply.provider_id
    and m.instance_num = supply.instance_num

where (basis.c_fullname is null or basis.c_fullname like '\PCORI_MOD\RX_BASIS\PR\%');

execute immediate 'create index prescribing_idx on prescribing (PATID, ENCOUNTERID)';
--GATHER_TABLE_STATS('PRESCRIBING');

end PCORNetPrescribing;
/
BEGIN
PCORNetPrescribing();
END;
/
SELECT count(PRESCRIBINGID) from prescribing where rownum = 1
--SELECT 1 FROM dual