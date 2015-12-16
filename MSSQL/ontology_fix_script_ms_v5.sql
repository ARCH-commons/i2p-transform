--clean metadata
--Updated jgk@20141212
--AHA@20140925 and jgk@20141212 and lcp@20141007
-- fix problems with pcornet metadata pcori_basecode column so the code AHA writes creates valid popmednet data

-- 1) There were missing PCORI_Basecodes in our procedures table v1.5. Run this to populate them
-- Retrieve pcori_basecode in proc from the path - retired codes were not reassigned pcori codes
update pcornet_proc set pcori_basecode=substring(c_fullname,len(c_fullname)+2-charindex('\',reverse(substring(c_fullname,1,len(c_fullname)-1))),5)
 where pcori_basecode is null and c_basecode is not null

-- 2) If you used the Mapper tool, then after running the integration step, run the following script to correct the pcori_basecode column:
update integration
set pcori_basecode = project_ont_mapping.destination_basecode from project_ont_mapping
where integration.c_basecode = project_ont_mapping.source_basecode
and integration.pcori_basecode is null
and project_ont_mapping.status_cd != 'D'
and integration.c_path = project_ont_mapping.destination_fullname;

-- 3) Run this script to fix some errors in pcori_basecode
-- Note: This has not changed since v4.1 - the new transformations can handle the scheme in PCORI_basecode

-- Remove scheme prefixes
update pcornet_demo set pcori_basecode=substring(c_basecode, charindex(':',c_basecode)+1,100 )	 where c_fullname like '\PCORI\DEMOGRAPHIC\SEX\%'
  or c_fullname like '\PCORI\DEMOGRAPHIC\RACE\%' or c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC\%';
-- Fix a problem with the hispanic codes
update pcornet_demo set pcori_basecode='Y' where pcori_basecode='HISPANIC';
update pcornet_demo set pcori_basecode='N' where pcori_basecode='NOTHISPANIC';
-- Scheme prefixes again
update pcornet_diag set pcori_basecode=substring(pcori_basecode, charindex(':',pcori_basecode)+1,100 )
  where c_fullname like '\PCORI\DIAGNOSIS\09\%' or c_fullname like '\PCORI_MOD\%';
-- Data truncation errors occur if modifier folders have a pcori_basecode
update pcornet_diag set pcori_basecode=null where c_visualattributes like 'O%';
-- Scheme prefixes
update pcornet_proc set pcori_basecode=substring(pcori_basecode, charindex(':',pcori_basecode)+1,100 )
  where c_fullname like '\PCORI\PROCEDURE\09\%';

