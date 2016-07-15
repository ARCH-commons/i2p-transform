---------------------------------------------------------------------------------------------------
-- i2b2-to-PCORNet Loader - PCORNET_MED Patch - MSSQL
---------------------------------------------------------------------------------------------------
-- Jeff Klann, PhD and
-- Aaron Abend, aabend@gmail.com, 978-621-7745
-- Version 1.0 - 2015-10-14  

----------------------------------------------------------------------------------------------------------------------------------------
--NOTE: This script alters data structures - it should be carefully reviewed before using
--      and it is best if each statement is run separately
--If you added local children to the ontology and did not use the Mapper/integration tool, update the sourcesystem_cd
-- in the procedures below.
--Run this on the database that contains your PCORnet_med ontology; it will add NDC and CUI columns.
-- If this takes more than 1 minute to run, your pcornet_med table likely has incorrect indexes
--
alter table pcornet_med add PCORI_CUI varchar(8) NULL 
GO
alter table pcornet_med add PCORI_NDC varchar(12) NULL
GO

-- Update the NDC codes for non-integration rows
update pcornet_med set pcori_ndc=pcori_basecode 
from pcornet_med where len(pcori_basecode)=11
 and c_hlevel>2 and sourcesystem_cd not in ('integration_tool') and pcori_basecode not like 'N%'
GO

-- Update RxNorm NDC codes for non-integration and non-RxNorm rows
update pcornet_med set pcori_cui=pcori_basecode
from pcornet_med where len(pcori_basecode)<11 
 and c_hlevel>2 and sourcesystem_cd not in ('integration_tool') and pcori_basecode not like 'N%' and m_applied_path='@' and c_basecode not like 'NDFRT%'
GO

-- Update integration and NDC rows for RxNorm 
with cui as 
( 
    select c_fullname,pcori_cui,c_hlevel from pcornet_med where pcori_cui is not null
   union all
    select m.c_fullname,cui.pcori_cui,m.c_hlevel from pcornet_med m
    inner join cui on cui.c_fullname=m.c_path where m.pcori_cui is null
), cuid as ( select c_fullname,pcori_cui, row_number() over (partition by C_FULLNAME order by c_hlevel desc) row from cui)
update m set m.pcori_cui=cuid.pcori_cui from pcornet_med m inner join cuid on cuid.c_fullname=m.c_fullname
 where cuid.row=1 and m.pcori_cui is null
GO

-- Update integration rows for NDC 
with ndc as 
( 
    select c_fullname,pcori_ndc,c_hlevel from pcornet_med where pcori_ndc is not null
   union all
    select m.c_fullname,ndc.pcori_ndc,m.c_hlevel from pcornet_med m
    inner join ndc on ndc.c_fullname=m.c_path where m.pcori_ndc is null
), ndcd as ( select c_fullname,pcori_ndc, row_number() over (partition by C_FULLNAME order by c_hlevel desc) row from ndc)
update m set m.pcori_ndc=ndcd.pcori_ndc from pcornet_med m inner join ndcd on ndcd.c_fullname=m.c_fullname
 where ndcd.row=1 and m.pcori_ndc is null
GO