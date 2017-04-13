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


/* Replace PCORNet ICD9, ICD10 diagnoses hierarchy with the local hierarchy filling in
the pcornet_basecode with the expected values.
*/

--select *
delete 
from "&&i2b2_meta_schema".PCORNET_DIAG
where c_fullname like '\PCORI\DIAGNOSIS\09\%'
   or c_fullname like '\PCORI\DIAGNOSIS\10\%'
;


insert into "&&i2b2_meta_schema".PCORNET_DIAG
-- TODO: Stop cheating by going back to Clarity
with edg9 as (
    select dx_id, code from clarity.edg_current_icd9@id
)
, edg10 as (
    select dx_id, code from clarity.edg_current_icd10@id
)
, heron_dx as (
  select case
         when ht.c_basecode like 'KUH|DX_ID:%'
         then to_number(replace(ht.c_basecode, 'KUH|DX_ID:', ''))
         else null
         end dx_id
       , ht.*
  from "&&i2b2_meta_schema"."&&terms_table" ht
  where c_fullname like '\i2b2\Diagnoses\ICD%'
)
, terms_dxi as (
  select case
         when ht.c_basecode like 'ICD9:%' then replace(ht.c_basecode, 'ICD9:', '')
         when ht.c_basecode like 'ICD10:%' then replace(ht.c_basecode, 'ICD10:', '')
         when ht.c_basecode like 'KUH|DX_ID:%'
          and ht.c_fullname like '\i2b2\Diagnoses\ICD9\%'
              then edg9.code
         when ht.c_basecode like 'KUH|DX_ID:%'
          and ht.c_fullname like '\i2b2\Diagnoses\ICD10\%'
              then edg10.code
         end pcori_basecode
       , ht.*
  from heron_dx ht
  left join edg9
         on edg9.dx_id = ht.dx_id
  left join edg10
         on edg10.dx_id = ht.dx_id
)
select
  td.c_hlevel,
  replace(replace(td.c_fullname, '\i2b2\Diagnoses\ICD9\', '\PCORI\DIAGNOSIS\09\')
                               , '\i2b2\Diagnoses\ICD10\', '\PCORI\DIAGNOSIS\10\') c_fullname, 
  td.c_name, td.c_synonym_cd, td.c_visualattributes,
  td.c_totalnum, td.c_basecode, td.c_metadataxml, td.c_facttablecolumn, td.c_tablename, 
  td.c_columnname, td.c_columndatatype, td.c_operator, td.c_dimcode, td.c_comment, 
  td.c_tooltip, td.m_applied_path, td.update_date, td.download_date, td.import_date, 
  td.sourcesystem_cd, td.valuetype_cd, td.m_exclusion_cd, td.c_path, td.c_symbol,
  td.pcori_basecode
from terms_dxi td
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
  


/** medication_id_to_best_rxcui

Spec for PRESCRIBING.RXNORM_CUI says:

  Where an RxNorm mapping exists for the source
  medication, this field contains the RxNorm concept
  identifier (CUI) at the highest possible specificity.

  If more than one option exists for mapping, the
  following ordered strategy may be adopted:
  1)Semantic generic clinical drug
  2)Semantic Branded clinical drug
  3)Generic drug pack
  4)Branded drug pack

*/
create or replace view medication_id_to_best_rxcui as
with pcornet_spec as (
  select '1) Semantic generic clinical drug (SCD)' spec_order, 1 ix, 'SCD' tty from dual union all
  select '2) Semantic Branded clinical drug (SBC)', 2, 'SBD' from dual union all
  select '3) Generic drug pack (GPCK)', 3, 'GPCK' from dual union all
  select '4) Branded drug pack (BPCK)', 4, 'BPCK' from dual
)
, cui_pref as (  -- Rx concepts joined with spec order
  select rxcui, str rxnorm_str, ix, spec_order
  from rxnorm.rxnconso@id con
  join pcornet_spec on pcornet_spec.tty = con.tty
)
/*
  TODO: Consider changing HERON paths to be RXCUIs or including the RXCUI column
  so that we don't have to reach back to the clarity_med_id_to_rxcui map. See
  also https://informatics.gpcnetwork.org/trac/Project/ticket/390.
*/
, clarity_med_id_to_rxcui as (
  select cmed.medication_id clarity_med_id, rxn.rxnorm_code rxcui, '1) Clarity'  dose_pref
  from clarity.rxnorm_codes@id rxn
  join clarity.clarity_medication@id cmed on cmed.medication_id = rxn.medication_id
  union all
  select clarity_med_id, rxcui, '2) GCN'
  from "&&i2b2_etl_schema".clarity_med_id_to_rxcui_gcn@id
  union all
  select clarity_med_id, rxcui, '3) NDC'
  from "&&i2b2_etl_schema".clarity_med_id_to_rxcui_ndc@id
)
, med_map_pref as ( -- HERON clarity_med_id_to_rxcui joined with spec order
    select distinct clarity_med_id medication_id
         , med_map.rxcui, cui_pref.rxnorm_str, med_map.dose_pref
         , coalesce(spec_order, '9) HERON mapping misc.') spec_order
    from clarity_med_id_to_rxcui med_map
    left join cui_pref on cui_pref.rxcui = med_map.rxcui
)
, med_map_best as (
  -- Take the mapping with the best (i.e. min) spec_order and dose_pref, just like...
  --  Taking the record with the max date
  --  http://stackoverflow.com/a/8898142
  -- This may result in multiple rxcuis per medication_id, so while we're
  -- at it, take the minimum rxcui among those with the best spec_order.
  select *
  from (select medication_id, rxcui, rxnorm_str, spec_order, dose_pref
             , rank() over (partition by medication_id order by spec_order, dose_pref, rxcui) rnk
        from med_map_pref)
  where rnk = 1
)
, all_med as (
  select cd. concept_cd, max(name_char) name_char
  from blueherondata.concept_dimension cd
  where cd.concept_cd like 'KUH|MEDICATION_ID:%'
  group by cd.concept_cd
)
select all_med.*, med_map_best.*
from all_med
left join med_map_best on all_med.concept_cd = 'KUH|MEDICATION_ID:' || med_map_best.medication_id
;
;
/* Eyeball it:

select *
from medication_id_to_best_rxcui mitr
join blueheronmetadata.counts_by_concept cbc on cbc.concept_cd = mitr.concept_cd
order by facts desc
;
*/

/* test cases (TODO: formalize these)

Picking out just one row per CUI from RXNORM can be tricky; did we goof?
select count(*), medication_id
from medication_id_to_best_rxcui
where medication_id is not null
group by medication_id having count(*) > 1;

How many of each spec_order?

select count(*), spec_order, dose_pref from medication_id_to_best_rxcui
group by spec_order, dose_pref order by 1 desc;

27825	1) Semantic generic clinical drug (SCD)	1) Clarity
17527	1) Semantic generic clinical drug (SCD)	2) GCN
 3475		 *null* lots of IVP. TODO: med mixes
 2888	9) HERON mapping misc.	2) GCN
 2550	9) HERON mapping misc.	1) Clarity
  421	1) Semantic generic clinical drug (SCD)	3) NDC
  295	3) Generic drug pack (GPCK)	1) Clarity
  177	3) Generic drug pack (GPCK)	2) GCN
   97	2) Semantic Branded clinical drug (SBC)	3) NDC
    4	9) HERON mapping misc.	3) NDC
    2	4) Branded drug pack (BPCK)	3) NDC
    1	3) Generic drug pack (GPCK)	3) NDC

Be sure RXCUI for 0.4 ML ENOXAPARIN 100 MG/ML SC SYRG includes the 0.4 ML dose info:
select concept_cd, name_char, rxcui, rxnorm_str, spec_order, dose_pref from medication_id_to_best_rxcui where concept_cd = 'KUH|MEDICATION_ID:85052'; -- -> 854235, 1) SCD

select concept_cd, name_char, rxcui, rxnorm_str, spec_order, dose_pref from medication_id_to_best_rxcui where concept_cd = 'KUH|MEDICATION_ID:85051'; -- -> 854248, 1) SCD

This one goes from "LEVOTHYROXINE PO" to "Levothyroxine Sodium 0.1 MG Oral Tablet"; is that right?
select concept_cd, name_char, rxcui, rxnorm_str, spec_order, dose_pref from medication_id_to_best_rxcui where concept_cd = 'KUH|MEDICATION_ID:150171'; -- -> 892246,	1) SCD
*/


insert into "&&i2b2_meta_schema".PCORNET_MED
with
terms_rx as (
  select 
    best.rxcui mapped_rxcui, ht.*
  from 
    "&&i2b2_meta_schema"."&&terms_table" ht
  left join medication_id_to_best_rxcui best on best.concept_cd = ht.c_basecode
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

/* Replace SCILHS PCORNet Labs hierarchy with our local LOINC hierarchy with
   adjustment to make it "SCILHS like".
*/

/* Remove existing SCIHLS Labs hierarchy */
truncate table "&&i2b2_meta_schema".pcornet_lab;

/* Insert LOINC codes from local hierarchy, setting c_basecode to appropriate
   LAB_NAME for common PCORNet labs. */
insert into "&&i2b2_meta_schema".pcornet_lab
select distinct
  lc.C_HLEVEL,
  replace(lc.C_FULLNAME, '\i2b2\Laboratory Tests\', '\PCORI\LAB_RESULT_CM\') c_fullname,
  lc.C_NAME,
  lc.C_SYNONYM_CD,
  lc.C_VISUALATTRIBUTES,
  lc.C_TOTALNUM,
  case
    when llm.lab_name is not null then 'LAB_NAME:'|| llm.lab_name
    else lc.c_basecode
  end c_basecode,
  to_char(lc.C_METADATAXML),
  lc.C_FACTTABLECOLUMN,
  lc.C_TABLENAME,
  lc.C_COLUMNNAME,
  lc.C_COLUMNDATATYPE,
  lc.C_OPERATOR,
  lc.C_DIMCODE,
  to_char(lc.C_COMMENT),
  lc.C_TOOLTIP,
  lc.M_APPLIED_PATH,
  lc.UPDATE_DATE,
  lc.DOWNLOAD_DATE,
  lc.IMPORT_DATE,
  lc.SOURCESYSTEM_CD,
  lc.VALUETYPE_CD,
  lc.M_EXCLUSION_CD,
  lc.C_PATH,
  lc.C_SYMBOL,
  llm.pcori_specimen_source,
  replace(lc.c_basecode, 'LOINC:', '') pcori_basecode
from "&&i2b2_meta_schema"."&&terms_table" lc
left outer join lab_loinc_mapping llm
  on lc.c_basecode = ('LOINC:' || llm.loinc_code)
where lc.c_basecode like 'LOINC:%'
;

/* Insert child KUH|COMPONENT_ID nodes, setting pcori_basecode, and c_path to
   that of it's parent. */
insert into "&&i2b2_meta_schema".pcornet_lab
with parent_loinc_codes as ( -- LOINC codes with children LOINC codes
  select p_loinc.*
  from "&&i2b2_meta_schema"."&&terms_table" p_loinc
  join "&&i2b2_meta_schema"."&&terms_table" c_loinc
    on c_loinc.c_fullname like (p_loinc.c_fullname || '%')
    and p_loinc.c_basecode like 'LOINC:%'
    and c_loinc.c_basecode like 'LOINC:%'
    and p_loinc.c_basecode!=c_loinc.c_basecode
)
, children_loinc_codes as ( -- LOINC codes without children LOINC codes
  select clc.*
  from "&&i2b2_meta_schema"."&&terms_table" clc
  where clc.c_basecode like 'LOINC:%'
    and clc.c_basecode not in (
      select c_basecode from parent_loinc_codes
  )
)
select distinct
  ccc.C_HLEVEL,
  replace(ccc.C_FULLNAME, '\i2b2\Laboratory Tests\', '\PCORI\LAB_RESULT_CM\') c_fullname,
  ccc.C_NAME,
  ccc.C_SYNONYM_CD,
  ccc.C_VISUALATTRIBUTES,
  ccc.C_TOTALNUM,
  ccc.C_BASECODE,
  to_char(ccc.C_METADATAXML),
  ccc.C_FACTTABLECOLUMN,
  ccc.C_TABLENAME,
  ccc.C_COLUMNNAME,
  ccc.C_COLUMNDATATYPE,
  ccc.C_OPERATOR,
  ccc.C_DIMCODE,
  to_char(ccc.C_COMMENT),
  ccc.C_TOOLTIP,
  ccc.M_APPLIED_PATH,
  ccc.UPDATE_DATE,
  ccc.DOWNLOAD_DATE,
  ccc.IMPORT_DATE,
  ccc.SOURCESYSTEM_CD,
  ccc.VALUETYPE_CD,
  ccc.M_EXCLUSION_CD,
  replace(clc.C_FULLNAME, '\i2b2\Laboratory Tests\', '\PCORI\LAB_RESULT_CM\') c_path,
  ccc.C_SYMBOL,
  llm.pcori_specimen_source,
  replace(clc.c_basecode, 'LOINC:', '') pcori_basecode
from children_loinc_codes clc
join "&&i2b2_meta_schema"."&&terms_table" ccc
  on ccc.c_fullname like (clc.c_fullname || '%')
  and ccc.c_basecode like 'KUH|COMPONENT_ID:%' -- TODO: Generalize for other sites.
left outer join lab_loinc_mapping llm
  on clc.c_basecode = ('LOINC:' || llm.loinc_code)
;

commit;

