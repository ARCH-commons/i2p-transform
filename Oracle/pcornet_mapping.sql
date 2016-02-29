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

commit;
