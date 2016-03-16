/* Adapted from work by Phillip Reeder at UTSW

Map local terms as leaves under the PCORNet terms.
*/

/* Delete any existing mapped terms
*/
set echo on;
--select *
delete 
from "&&i2b2_meta_schema".PCORNET_VITAL where sourcesystem_cd='MAPPING';

insert into "&&i2b2_meta_schema".PCORNET_VITAL
SELECT PCORNET_VITAL.C_HLEVEL+1,
  PCORNET_VITAL.C_FULLNAME || i2b2.c_basecode || '\' as  C_FULLNAME,
  i2b2.c_basecode || ' ' || i2b2.c_name as C_NAME,
  PCORNET_VITAL.C_SYNONYM_CD,
  PCORNET_VITAL.C_VISUALATTRIBUTES,
  PCORNET_VITAL.C_TOTALNUM,
  i2b2.c_basecode as C_BASECODE,
  PCORNET_VITAL.C_METADATAXML,
  PCORNET_VITAL.C_FACTTABLECOLUMN,
  PCORNET_VITAL.C_TABLENAME,
  PCORNET_VITAL.C_COLUMNNAME,
  PCORNET_VITAL.C_COLUMNDATATYPE,
  PCORNET_VITAL.C_OPERATOR,
  PCORNET_VITAL.C_FULLNAME || i2b2.c_basecode || '\' as  C_DIMCODE,
  PCORNET_VITAL.C_COMMENT,
  PCORNET_VITAL.C_TOOLTIP,
  PCORNET_VITAL.M_APPLIED_PATH,
  PCORNET_VITAL.UPDATE_DATE,
  PCORNET_VITAL.DOWNLOAD_DATE,
  PCORNET_VITAL.IMPORT_DATE,
  'MAPPING' as SOURCESYSTEM_CD,
  PCORNET_VITAL.VALUETYPE_CD,
  PCORNET_VITAL.M_EXCLUSION_CD,
  PCORNET_VITAL.C_PATH,
  PCORNET_VITAL.C_SYMBOL,
  PCORNET_VITAL.PCORI_BASECODE
FROM "&&i2b2_meta_schema".PCORNET_VITAL join pcornet_mapping on pcornet_mapping.PCORI_PATH=PCORNET_VITAL.c_fullname and pcornet_mapping.local_path is not null
join "&&i2b2_meta_schema"."&&terms_table" i2b2 on i2b2.c_fullname=pcornet_mapping.local_path;

insert into "&&i2b2_meta_schema".PCORNET_ENC
SELECT PCORNET_ENC.C_HLEVEL+1,
  PCORNET_ENC.C_FULLNAME || i2b2.c_name || '\' as  C_FULLNAME,
  i2b2.c_basecode || ' ' || i2b2.c_name as C_NAME,
  PCORNET_ENC.C_SYNONYM_CD,
  PCORNET_ENC.C_VISUALATTRIBUTES,
  PCORNET_ENC.C_TOTALNUM,
  i2b2.c_basecode as C_BASECODE,
  PCORNET_ENC.C_METADATAXML,
  PCORNET_ENC.C_FACTTABLECOLUMN,
  PCORNET_ENC.C_TABLENAME,
  PCORNET_ENC.C_COLUMNNAME,
  PCORNET_ENC.C_COLUMNDATATYPE,
  PCORNET_ENC.C_OPERATOR,
  PCORNET_ENC.C_FULLNAME || i2b2.c_name || '\' as  C_DIMCODE,
  PCORNET_ENC.C_COMMENT,
  PCORNET_ENC.C_TOOLTIP,
  PCORNET_ENC.M_APPLIED_PATH,
  PCORNET_ENC.UPDATE_DATE,
  PCORNET_ENC.DOWNLOAD_DATE,
  PCORNET_ENC.IMPORT_DATE,
  'MAPPING' as SOURCESYSTEM_CD,
  PCORNET_ENC.VALUETYPE_CD,
  PCORNET_ENC.M_EXCLUSION_CD,
  PCORNET_ENC.C_PATH,
  PCORNET_ENC.C_SYMBOL,
  PCORNET_ENC.PCORI_BASECODE
FROM "&&i2b2_meta_schema".PCORNET_ENC join pcornet_mapping on pcornet_mapping.PCORI_PATH=PCORNET_ENC.c_fullname and pcornet_mapping.local_path is not null
join "&&i2b2_meta_schema"."&&terms_table" i2b2 on i2b2.c_fullname=pcornet_mapping.local_path;

commit;

/* Updates to PCORNet demographic ontology
 - Hispanic was added as a column, so updating code lists.
 
TODO: Consider making a pull request to the SCILHS ontology with the changes.
See also: https://github.com/SCILHS/i2p-transform/issues/1
*/
delete
from "&&i2b2_meta_schema".pcornet_demo where c_fullname in (
  select c_fullname from pcornet_ontology_updates
  where c_fullname like '\PCORI\DEMOGRAPHIC\%'
  );
insert into "&&i2b2_meta_schema".pcornet_demo (
  select * from pcornet_ontology_updates where c_fullname like '\PCORI\DEMOGRAPHIC\%'
  );

/* Bugs in the SCILHS ontology
*/
--https://github.com/SCILHS/scilhs-ontology/issues/11
update "&&i2b2_meta_schema".pcornet_diag pd set pd.pcori_basecode = 'PDX:S' where c_fullname = '\PCORI_MOD\PDX\S\';
update "&&i2b2_meta_schema".pcornet_diag pd set pd.pcori_basecode = 'PDX:P' where c_fullname = '\PCORI_MOD\PDX\P\';
update "&&i2b2_meta_schema".pcornet_diag pd set pd.pcori_basecode = 'PDX:X' where c_fullname = '\PCORI_MOD\PDX\X\';


/* Replace PCORNet ICD9 diagnoses hierarchy with the local hierarchy filling in
the pcornet_basecode with the expected values.
*/

--select *
delete 
from "&&i2b2_meta_schema".PCORNET_DIAG
where c_fullname like '\PCORI\DIAGNOSIS\09\%'
;


insert into "&&i2b2_meta_schema".PCORNET_DIAG
with terms_dxi as (
  select 
    cicd.code dxicd, ht.* 
  from 
    "&&i2b2_meta_schema"."&&terms_table" ht
  -- TODO: Stop cheating by going back to Clarity
  left join clarity.edg_current_icd9@id cicd on to_char(cicd.dx_id) = replace(ht.c_basecode, 'KUH|DX_ID:', '')
  where c_fullname like '\i2b2\Diagnoses\ICD9\%' order by c_hlevel 
  )
select
  td.c_hlevel, 
  replace(td.c_fullname, '\i2b2\Diagnoses\ICD9\', '\PCORI\DIAGNOSIS\09\') c_fullname, 
  td.c_name, td.c_synonym_cd, td.c_visualattributes,
  td.c_totalnum, td.c_basecode, td.c_metadataxml, td.c_facttablecolumn, td.c_tablename, 
  td.c_columnname, td.c_columndatatype, td.c_operator, td.c_dimcode, td.c_comment, 
  td.c_tooltip, td.m_applied_path, td.update_date, td.download_date, td.import_date, 
  td.sourcesystem_cd, td.valuetype_cd, td.m_exclusion_cd, td.c_path, td.c_symbol,
  case 
    when td.dxicd is not null then td.dxicd
    when td.c_basecode like 'ICD9:%' then replace(td.c_basecode, 'ICD9:', '')
    else null 
  end pcori_basecode
from terms_dxi td
order by c_hlevel
;


-- Other diagnosis mappings such as modifiers for PDX, DX_SOURCE.
insert into "&&i2b2_meta_schema".PCORNET_DIAG
SELECT pcornet_diag.C_HLEVEL+1,
  pcornet_diag.C_FULLNAME || i2b2.c_basecode || '\' as  C_FULLNAME,
  i2b2.c_basecode || ' ' || i2b2.c_name as C_NAME,
  pcornet_diag.C_SYNONYM_CD,
  pcornet_diag.C_VISUALATTRIBUTES,
  pcornet_diag.C_TOTALNUM,
  i2b2.c_basecode as C_BASECODE,
  pcornet_diag.C_METADATAXML,
  pcornet_diag.C_FACTTABLECOLUMN,
  pcornet_diag.C_TABLENAME,
  pcornet_diag.C_COLUMNNAME,
  pcornet_diag.C_COLUMNDATATYPE,
  pcornet_diag.C_OPERATOR,
  pcornet_diag.C_FULLNAME || i2b2.c_basecode || '\' as  C_DIMCODE,
  pcornet_diag.C_COMMENT,
  pcornet_diag.C_TOOLTIP,
  pcornet_diag.M_APPLIED_PATH,
  pcornet_diag.UPDATE_DATE,
  pcornet_diag.DOWNLOAD_DATE,
  pcornet_diag.IMPORT_DATE,
  'MAPPING' as SOURCESYSTEM_CD,
  pcornet_diag.VALUETYPE_CD,
  pcornet_diag.M_EXCLUSION_CD,
  pcornet_diag.C_PATH,
  pcornet_diag.C_SYMBOL,
  pcornet_diag.PCORI_BASECODE
FROM "&&i2b2_meta_schema".pcornet_diag join pcornet_mapping on pcornet_mapping.PCORI_PATH = pcornet_diag.c_fullname and pcornet_mapping.local_path is not null
join "&&i2b2_meta_schema"."&&terms_table" i2b2 on i2b2.c_fullname like pcornet_mapping.local_path || '%';

commit;

--select *
delete 
from "&&i2b2_meta_schema".PCORNET_PROC
where c_fullname like '\PCORI\PROCEDURE\09\_%'
;

/* Replace PCORNet Procedure (ICD9) hierarchy with the local hierarchy.
*/
insert into "&&i2b2_meta_schema".PCORNET_PROC
select 
  ht.c_hlevel, 
  replace(ht.c_fullname, '\i2b2\Procedures\PRC\ICD9 (Inpatient)', '\PCORI\PROCEDURE\09') c_fullname, 
  ht.c_name, ht.c_synonym_cd, ht.c_visualattributes,
  ht.c_totalnum, ht.c_basecode, ht.c_metadataxml, ht.c_facttablecolumn, ht.c_tablename, 
  ht.c_columnname, ht.c_columndatatype, ht.c_operator, ht.c_dimcode, ht.c_comment, 
  ht.c_tooltip, ht.m_applied_path, ht.update_date, ht.download_date, ht.import_date, 
  ht.sourcesystem_cd, ht.valuetype_cd, ht.m_exclusion_cd, ht.c_path, ht.c_symbol,
  ht.c_basecode pcori_basecode 
from 
  "&&i2b2_meta_schema"."&&terms_table" ht
where c_fullname like '\i2b2\Procedures\PRC\ICD9 (Inpatient)\_%' order by c_hlevel 
;


--select *
delete 
from "&&i2b2_meta_schema".PCORNET_PROC
where c_fullname like '\PCORI\PROCEDURE\C4\%'
;

/* Replace PCORNet Procedure (ICD9) hierarchy with the local hierarchy.
*/
insert into "&&i2b2_meta_schema".PCORNET_PROC
select 
  ht.c_hlevel, 
  replace(ht.c_fullname, '\i2b2\Procedures\PRC\Metathesaurus CPT Hierarchical Terms', '\PCORI\PROCEDURE\C4') c_fullname, 
  ht.c_name, ht.c_synonym_cd, ht.c_visualattributes,
  ht.c_totalnum, ht.c_basecode, ht.c_metadataxml, ht.c_facttablecolumn, ht.c_tablename, 
  ht.c_columnname, ht.c_columndatatype, ht.c_operator, ht.c_dimcode, ht.c_comment, 
  ht.c_tooltip, ht.m_applied_path, ht.update_date, ht.download_date, ht.import_date, 
  ht.sourcesystem_cd, ht.valuetype_cd, ht.m_exclusion_cd, ht.c_path, ht.c_symbol,
  ht.c_basecode pcori_basecode 
from 
  "&&i2b2_meta_schema"."&&terms_table" ht
where c_fullname like '\i2b2\Procedures\PRC\Metathesaurus CPT Hierarchical Terms\%' order by c_hlevel 
;

/* MS-DRGs
*/
delete 
from "&&i2b2_meta_schema".PCORNET_ENC
where c_fullname like '\PCORI\ENCOUNTER\DRG\02\%'
;

insert into "&&i2b2_meta_schema".PCORNET_ENC
with drg_path_map as (
  select local_path, local_path || '%' like_local_path, pcori_path, pcori_path || '%' like_pcori_path
  from pcornet_mapping where pcori_path like '\PCORI\ENCOUNTER\DRG\02\%'
  )
select 
  ht.c_hlevel, 
  replace(ht.c_fullname, pm.local_path, pm.pcori_path) c_fullname, 
  ht.c_name, ht.c_synonym_cd, ht.c_visualattributes,
  ht.c_totalnum, ht.c_basecode, ht.c_metadataxml, ht.c_facttablecolumn, ht.c_tablename, 
  ht.c_columnname, ht.c_columndatatype, ht.c_operator, ht.c_dimcode, ht.c_comment, 
  ht.c_tooltip, ht.m_applied_path, ht.update_date, ht.download_date, ht.import_date, 
  ht.sourcesystem_cd, ht.valuetype_cd, ht.m_exclusion_cd, ht.c_path, ht.c_symbol,
  --TODO: Consider derviving the appropriate replacement strings.
  replace(ht.c_basecode, 'UHC|UHCMSDRG:', 'MSDRG:') pcori_basecode 
from 
  "&&i2b2_meta_schema"."&&terms_table" ht
cross join drg_path_map pm
where c_fullname like pm.like_local_path
--order by c_hlevel 
;

/* The following is good for eyeballing the DRG alignment - the names seem to
match exactly between UHC and the PCORNet ontology.
*/
/*
with drg_path_map as (
  select local_path, local_path || '%' like_local_path, pcori_path, pcori_path || '%' like_pcori_path
  from pcornet_mapping where pcori_path like '\PCORI\ENCOUNTER\DRG\02\%'
  ),
pcornet_drgs as (
  select replace(pe.pcori_basecode, 'MSDRG:', '') drg_code, pe.* 
  from "&&i2b2_meta_schema".pcornet_enc pe
  cross join drg_path_map dm
  where pe.c_fullname like dm.like_pcori_path
  ),
local_drgs as (
  select
    replace(ht.c_basecode, 'UHC|UHCMSDRG:', '') drg_code, ht.*
  from "&&i2b2_meta_schema"."&&terms_table" ht 
  cross join drg_path_map dm
  where ht.c_fullname like dm.like_local_path
  )
select pd.drg_code, pd.c_name, ld.c_name from pcornet_drgs pd
join local_drgs ld on ld.drg_code = pd.drg_code;
*/
