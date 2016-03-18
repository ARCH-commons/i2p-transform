-- Do the following before running this script:
-- If using Oracle:
-- 	Execute scilhs-ontology/Release/Oracle/create_oracle_metadata_tables.sql
-- 	Populate I2B2METADATA.pcornet_med using scilhs-ontology/Release/Oracle/pcornet_med.zip

-- Do the following after running this script:
-- Execute i2p-transform/Oracle/PCORNetLoader_ora.sql

-- alter table pcornet_med add PCORI_NDC varchar2(12);
DECLARE
  X NUMBER;
BEGIN
  SELECT COUNT(*)
    INTO X 
    FROM dba_tab_columns
    WHERE owner = 'I2B2METADATA'
    AND table_name = 'PCORNET_MED'
    AND column_name = 'PCORI_NDC';
  IF (X = 0) THEN
    EXECUTE IMMEDIATE 'ALTER TABLE I2B2METADATA.pcornet_med ADD PCORI_NDC varchar2(12) NULL';
  END IF;
END;

UPDATE I2B2METADATA.PCORNET_MED set pcori_ndc = pcori_basecode where length(pcori_basecode)=11 and c_hlevel>2 and LOWER(sourcesystem_cd) not in ('integration_tool')
and pcori_basecode not like 'N%';
 
-- alter table pcornet_med add PCORI_CUI varchar2(8);
DECLARE
  X NUMBER;
BEGIN
  SELECT COUNT(*)
    INTO X 
    FROM dba_tab_columns
    WHERE owner = 'I2B2METADATA'
    AND table_name = 'PCORNET_MED'
    AND column_name = 'PCORI_CUI';
  IF (X = 0) THEN
    EXECUTE IMMEDIATE 'ALTER TABLE I2B2METADATA.pcornet_med ADD PCORI_CUI varchar2(8) NULL';
  END IF;
END;

-- necessary for CDM scripts if modifying ONT cell schema
grant all on I2B2METADATA.pcornet_med to cdm;

UPDATE I2B2METADATA.pcornet_med set PCORI_CUI = PCORI_BASECODE where length(pcori_basecode)<11 and c_hlevel>2 and LOWER(sourcesystem_cd) not in ('integration_tool')
and pcori_basecode not like 'N%' and m_applied_path='@';

alter session set current_schema=CDM;
 
create table CUI_T  as
select * from
(
with
cui(c_fullname,pcori_cui,c_hlevel) as
(
    select c_fullname,pcori_cui,c_hlevel from pcornet_med where pcori_cui is not null
   union all
    select m.c_fullname, cui.pcori_cui, m.c_hlevel
    from I2B2METADATA.pcornet_med m inner join cui on cui.c_fullname=m.c_path where m.pcori_cui is null
),
cuid as
( select c_fullname, pcori_cui, row_number() over (partition by C_FULLNAME order by c_hlevel desc)as rowno  from cui)
 
select DISTINCT cuid.c_fullname as c_fullname, cuid.pcori_cui as pcori_cui from cuid where cuid.rowno=1
);

-- TODO: work on performance
update I2B2METADATA.pcornet_med x set x.pcori_cui =
    (select pcori_cui  from CUI_T t where x.c_fullname = t.c_fullname)
where x.pcori_cui is null;
 
 
 

create table NDC_T as
select * from
(
with ndc(c_fullname,pcori_ndc,c_hlevel) as
(
    select c_fullname,pcori_ndc,c_hlevel from pcornet_med where pcori_ndc is not null
   union all
    select m.c_fullname,ndc.pcori_ndc,m.c_hlevel from pcornet_med m
    inner join ndc on ndc.c_fullname=m.c_path where m.pcori_ndc is null
),
ndcd as ( select c_fullname,pcori_ndc, row_number() over (partition by C_FULLNAME order by c_hlevel desc) as rowno from ndc)
select DISTINCT ndcd.c_fullname as c_fullname, ndcd.pcori_ndc as pcori_ndc from ndcd where ndcd.rowno=1
);
 
-- TODO: work on performance
update I2B2METADATA.pcornet_med x set x.pcori_ndc =
    (select pcori_ndc  from NDC_T t where x.c_fullname = t.c_fullname)
where x.pcori_ndc is null;

