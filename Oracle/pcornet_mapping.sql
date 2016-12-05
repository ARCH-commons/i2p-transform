/* Adapted from work by Phillip Reeder at UTSW

Map local terms as leaves under the PCORNet terms.
*/

/* Delete any existing mapped terms
*/
set echo on;
--select *
delete 
from "&&i2b2_meta_schema".PCORNET_VITAL where sourcesystem_cd='MAPPING';

delete 
from "&&i2b2_meta_schema".PCORNET_ENC where sourcesystem_cd='MAPPING';

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

-- TODO may be better represented in pcornet_mapping.csv
create or replace view proc_local_to_pcori as
select           '\PCORI\PROCEDURE\09\' as pcori_path, '\i2b2\Procedures\PRC\ICD9 (Inpatient)\' as local_path from dual 
union all select '\PCORI\PROCEDURE\10\' as pcori_path, '\i2b2\Procedures\ICD10\' as local_path from dual 
union all select '\PCORI\PROCEDURE\C4\' as pcori_path, '\i2b2\Procedures\PRC\Metathesaurus CPT Hierarchical Terms\' as local_path from dual
;

--select *
delete
from "&&i2b2_meta_schema".PCORNET_PROC p_proc
where exists (select 1
              from proc_local_to_pcori m
              where p_proc.c_fullname like m.pcori_path || '%');

insert into "&&i2b2_meta_schema".PCORNET_PROC
select 
  ht.c_hlevel, 
  replace(ht.c_fullname, m.local_path, m.pcori_path) c_fullname, 
  ht.c_name, ht.c_synonym_cd, ht.c_visualattributes,
  ht.c_totalnum, ht.c_basecode, ht.c_metadataxml, ht.c_facttablecolumn, ht.c_tablename, 
  ht.c_columnname, ht.c_columndatatype, ht.c_operator, ht.c_dimcode, ht.c_comment, 
  ht.c_tooltip, ht.m_applied_path, ht.update_date, ht.download_date, ht.import_date, 
  ht.sourcesystem_cd, ht.valuetype_cd, ht.m_exclusion_cd, ht.c_path, ht.c_symbol,
  ht.c_basecode pcori_basecode
from  "&&i2b2_meta_schema"."&&terms_table" ht
join proc_local_to_pcori m on ht.c_fullname like m.local_path || '%'
order by ht.c_hlevel;

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
  case when ht.c_basecode is not null 
    then 'MSDRG:' || lpad(substr(ht.c_basecode, instr(ht.c_basecode, ':') + 1), 3, '0')
  end pcori_basecode
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

/* Admitting source: For i2p-transform, these values go in the visit dimension.
To make them queryable from the web interface, insert the local terms under the
PCORI parent.
*/


create or replace view admit_src_path_map as
select local_path, local_path || '%' like_local_path, pcori_path, pcori_path || '%' like_pcori_path
from pcornet_mapping where pcori_path like '\PCORI\ENCOUNTER\ADMITTING_SOURCE\%'
;

update "&&i2b2_meta_schema".PCORNET_ENC
set c_visualattributes = 'FA' where c_fullname in (
  select pcori_path from admit_src_path_map
  );
  
insert into "&&i2b2_meta_schema".PCORNET_ENC
select 
  pe.c_hlevel + 1 c_hlevel,
  pm.pcori_path || ht.c_basecode || '\' c_fullname,
  ht.c_name, ht.c_synonym_cd, ht.c_visualattributes,
  ht.c_totalnum, ht.c_basecode, ht.c_metadataxml, ht.c_facttablecolumn, ht.c_tablename, 
  ht.c_columnname, ht.c_columndatatype, ht.c_operator, ht.c_dimcode, ht.c_comment, 
  ht.c_tooltip, ht.m_applied_path, ht.update_date, ht.download_date, ht.import_date, 
  'MAPPING' sourcesystem_cd, ht.valuetype_cd, ht.m_exclusion_cd, ht.c_path, ht.c_symbol,
  pe.pcori_basecode
from 
  "&&i2b2_meta_schema"."&&terms_table" ht
cross join admit_src_path_map pm
join "&&i2b2_meta_schema".pcornet_enc pe on pe.c_fullname = pm.pcori_path
where ht.c_fullname like pm.like_local_path
order by c_hlevel
;


/* Medications
This section takes the place of PCORI_MEDS_SCHEMA_CHANGE_ora.sql included in the
SCILHS code.
*/
whenever sqlerror continue;
alter table "&&i2b2_meta_schema".pcornet_med add (
  pcori_cui varchar2(8)
  );

alter table "&&i2b2_meta_schema".pcornet_med add (
  pcori_ndc varchar2(12)
  );
whenever sqlerror exit;

delete 
from "&&i2b2_meta_schema".PCORNET_MED
where c_fullname like '\PCORI\MEDICATION\RXNORM_CUI\%'
and c_fullname not like '\PCORI_MOD\%';

update "&&i2b2_meta_schema".PCORNET_MED med
set med.c_visualattributes = 'DAE' where c_fullname in (
  select distinct pm.pcori_path
  from pcornet_mapping pm
  join "&&i2b2_meta_schema".PCORNET_MED med on med.c_fullname = pm.pcori_path
  where  med.c_fullname like '\PCORI_MOD\%'
  );
  
/* RX Frequency modifiers
*/
create or replace view rx_frequency_strs as
select '\PCORI_MOD\RX_FREQUENCY\' freq_mod_path, 'RX_FREQUENCY:' freq_mod_pfx from dual;

delete
from "&&i2b2_meta_schema".PCORNET_MED pm
where pm.c_fullname like (select strs.freq_mod_path || '%' from rx_frequency_strs strs);

insert into "&&i2b2_meta_schema".pcornet_med
select
  c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_totalnum, 
  c_basecode, c_metadataxml, c_facttablecolumn, c_tablename, c_columnname, 
  c_columndatatype, c_operator, c_dimcode, c_comment, c_tooltip, m_applied_path, 
  update_date, download_date, import_date, sourcesystem_cd, valuetype_cd, 
  m_exclusion_cd, c_path, c_symbol, 
  strs.freq_mod_pfx || substr(ht.c_fullname, length(strs.freq_mod_path) + 1, 2) pcori_basecode, 
  null pcori_cui, null pcori_ndc
from "&&i2b2_meta_schema"."&&terms_table" ht
cross join rx_frequency_strs strs
where ht.c_fullname like (select strs.freq_mod_path || '%' from rx_frequency_strs)
and ht.m_exclusion_cd is null
and ht.c_basecode is not null;
  

insert into "&&i2b2_meta_schema".PCORNET_MED
with 
rxnorm_mapping as (
  /* TODO: Consider whether we want just one rxcui for a clarity medication?
  Without picking just one this query results in duplicate c_fullnames and causes
  errors in the webclient.
  TODO: Consider changing HERON paths to be RXCUIs or including the RXCUI column
  so that we don't have to reach back to the clarity_med_id_to_rxcui map. See
  also https://informatics.gpcnetwork.org/trac/Project/ticket/390.
  */
  select min(rxcui) rxcui, clarity_med_id 
  from "&&i2b2_etl_schema". clarity_med_id_to_rxcui@id
  group by clarity_med_id
  ),
terms_rx as (
  select 
    cm2rx.rxcui mapped_rxcui, ht.* 
  from 
    "&&i2b2_meta_schema"."&&terms_table" ht
  left join rxnorm_mapping cm2rx on to_char(cm2rx.clarity_med_id) = replace(ht.c_basecode, 'KUH|MEDICATION_ID:', '')
  where c_fullname like '\i2b2\Medications%'
		and c_basecode not like 'NDC:%' -- We'll handle NDCs seperately below
  	and ht.c_visualattributes not like '%H%'
  )
select
  rx.c_hlevel + 1 c_hlevel, 
  replace(rx.c_fullname, '\i2b2\Medications\', '\PCORI\MEDICATION\RXNORM_CUI\') c_fullname, 
  rx.c_name, rx.c_synonym_cd, rx.c_visualattributes,
  rx.c_totalnum, rx.c_basecode, rx.c_metadataxml, rx.c_facttablecolumn, rx.c_tablename, 
  rx.c_columnname, rx.c_columndatatype, rx.c_operator, rx.c_dimcode, rx.c_comment, 
  rx.c_tooltip, rx.m_applied_path, rx.update_date, rx.download_date, rx.import_date, 
  rx.sourcesystem_cd, rx.valuetype_cd, rx.m_exclusion_cd, rx.c_path, rx.c_symbol,
  rx.rxcui pcori_basecode, rx.rxcui pcori_cui, 
  -- Don't worry about NDCs for now - used for dispensing
  null pcori_ndc from (
    select
      trx.*,
      case 
        when trx.mapped_rxcui is not null then trx.mapped_rxcui
        when trx.c_basecode like 'RXCUI:%' then replace(trx.c_basecode, 'RXCUI:', '')
        else null 
      end rxcui
    from terms_rx trx
    --order by trx.c_hlevel
    ) rx;

-- Handle mapping NDC codes for DISPENSING
insert into "&&i2b2_meta_schema".PCORNET_MED
select
	c_hlevel + 1 c_hlevel,
	replace(c_fullname, '\i2b2\Medications\', '\PCORI\MEDICATION\RXNORM_CUI\') c_fullname,
  c_name, c_synonym_cd, c_visualattributes,
  c_totalnum, c_basecode, c_metadataxml, c_facttablecolumn, c_tablename,
  c_columnname, c_columndatatype, c_operator, c_dimcode, c_comment,
  c_tooltip, m_applied_path, update_date, download_date, import_date,
  sourcesystem_cd, valuetype_cd, m_exclusion_cd, c_path, c_symbol,
  null pcori_basecode, null pcori_cui,
  substr(c_basecode, 5) pcori_ndc
from "&&i2b2_meta_schema"."&&terms_table"
where c_fullname like '\i2b2\Medications%'
	and c_basecode like 'NDC:%'
	and length(substr(c_basecode, 5)) < 12 -- Make sure we have 11 digit NDC
;

delete 
from "&&i2b2_meta_schema".PCORNET_MED where sourcesystem_cd='MAPPING';


insert into "&&i2b2_meta_schema".PCORNET_MED
with med_mod_mapping as (
  select distinct pcori_path, i2b2.c_fullname, i2b2.c_basecode, i2b2.c_name
  FROM "&&i2b2_meta_schema".pcornet_med 
  join pcornet_mapping on pcornet_mapping.PCORI_PATH = pcornet_med.c_fullname and pcornet_mapping.local_path is not null
  join "&&i2b2_meta_schema"."&&terms_table" i2b2 on i2b2.c_fullname like pcornet_mapping.local_path || '%'
  where i2b2.c_basecode is not null
  )
SELECT pcornet_med.C_HLEVEL+1,
  pcornet_med.C_FULLNAME || med_mod_mapping.c_basecode || '\' as  C_FULLNAME,
  med_mod_mapping.c_basecode || ' ' || med_mod_mapping.c_name as C_NAME,
  pcornet_med.C_SYNONYM_CD,
  pcornet_med.C_VISUALATTRIBUTES,
  pcornet_med.C_TOTALNUM,
  med_mod_mapping.c_basecode as C_BASECODE,
  pcornet_med.C_METADATAXML,
  pcornet_med.C_FACTTABLECOLUMN,
  pcornet_med.C_TABLENAME,
  pcornet_med.C_COLUMNNAME,
  pcornet_med.C_COLUMNDATATYPE,
  pcornet_med.C_OPERATOR,
  pcornet_med.C_FULLNAME || med_mod_mapping.c_basecode || '\' as  C_DIMCODE,
  pcornet_med.C_COMMENT,
  pcornet_med.C_TOOLTIP,
  pcornet_med.M_APPLIED_PATH,
  pcornet_med.UPDATE_DATE,
  pcornet_med.DOWNLOAD_DATE,
  pcornet_med.IMPORT_DATE,
  'MAPPING' as SOURCESYSTEM_CD,
  pcornet_med.VALUETYPE_CD,
  pcornet_med.M_EXCLUSION_CD,
  pcornet_med.C_PATH,
  pcornet_med.C_SYMBOL,
  pcornet_med.PCORI_BASECODE,
  null pcori_cui,
  null pcori_ndc
from med_mod_mapping
join "&&i2b2_meta_schema".pcornet_med on med_mod_mapping.PCORI_PATH = pcornet_med.c_fullname
;

commit;

/* Add relevent nodes from local i2b2 lab hierarchy to PCORNet Labs hierarchy.
*/

insert into "&&i2b2_meta_schema".pcornet_lab
with lab_map as (
	select distinct lab.c_hlevel, lab.c_path, lab.pcori_specimen_source, trim(CHR(13) from lab.pcori_basecode) as pcori_basecode
  from "&&i2b2_meta_schema".pcornet_lab lab 
  inner JOIN "&&i2b2_meta_schema".pcornet_lab ont_parent on lab.c_path=ont_parent.c_fullname
  inner join pmn_labnormal norm on ont_parent.c_basecode=norm.LAB_NAME
  where lab.c_fullname like '\PCORI\LAB_RESULT_CM\%'
),
local_loinc_terms as (
  select lm.C_HLEVEL, lt.C_FULLNAME, lm.C_PATH, lm.PCORI_BASECODE, lm.pcori_specimen_source
  from "&&i2b2_meta_schema"."&&terms_table" lt, lab_map lm
  where lm.pcori_basecode=replace(lt.c_basecode, 'LOINC:', '')
    and lt.c_fullname like '\i2b2\Laboratory Tests\%' and lt.c_basecode like 'LOINC:%'
)
select 
  llt.C_HLEVEL,
  concat(llt.c_path, substr(regexp_substr(lt.c_fullname, '\\[^\\]+\\$'), 2, length(regexp_substr(lt.c_fullname, '\\[^\\]+\\$')))) as c_fullname,
  lt.C_NAME,
  lt.C_SYNONYM_CD,
  lt.C_VISUALATTRIBUTES,
  lt.C_TOTALNUM,
  lt.C_BASECODE,
  lt.C_METADATAXML,
  lt.C_FACTTABLECOLUMN,
  lt.C_TABLENAME,
  lt.C_COLUMNNAME,
  lt.C_COLUMNDATATYPE,
  lt.C_OPERATOR,
  lt.C_DIMCODE,
  lt.C_COMMENT,
  lt.C_TOOLTIP,
  lt.M_APPLIED_PATH,
  lt.UPDATE_DATE,
  lt.DOWNLOAD_DATE,
  lt.IMPORT_DATE,
  'MAPPING',
  lt.VALUETYPE_CD,
  lt.M_EXCLUSION_CD,
  llt.C_PATH,
  lt.C_SYMBOL,
  llt.pcori_specimen_source,
  llt.PCORI_BASECODE
from "&&i2b2_meta_schema"."&&terms_table" lt, local_loinc_terms llt
where lt.c_fullname like llt.c_fullname||'%\'
;

commit;

