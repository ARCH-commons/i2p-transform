/** death - create and populate the death table.
*/
insert into cdm_status (task, start_time) select 'death', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE death');
END;
/
CREATE TABLE death(
	PATID varchar(50) NOT NULL,
	DEATH_DATE date NOT NULL,
	DEATH_DATE_IMPUTE varchar(2) NULL,
	DEATH_SOURCE varchar(2) NOT NULL,
	DEATH_MATCH_CONFIDENCE varchar(2) NULL
)
/
create or replace procedure PCORNetDeath as
begin

execute immediate 'truncate table death';

insert into death(patid, death_date, death_date_impute, death_source, death_match_confidence)
with death_source_map as (
 select 'DEM|VITAL:y' concept_cd, 'L' death_source, null unknown from dual
 union all
 select 'DEM|VITAL:yu' concept_cd, 'L' death_source, 1 unknown from dual
 union all
 select 'DEM|VITAL|SSA:y' concept_cd, 'D' death_source, null unknown from dual
 union all
 select 'NAACCR|1760:0' concept_cd, 'T' death_source, null unknown from dual
 union all
 select 'NAACCR|1760:4' concept_cd, 'T' death_source, null unknown from dual
 -- Possible exception, NAACCR|1760:12 is not handled here as there are no examples in the data.
 -- This is the case where date of death is flagged as unknown.
)
select distinct /*+ parallel(0) */ obs.patient_num patid
     , case when dmap.unknown = 1 then DATE '2100-12-31' else obs.start_date end death_date
     , case when dmap.unknown = 1 then 'OT' else 'N' end death_date_impute
     , dmap.death_source
     , 'NI' death_match_confidence
from pcornet_cdm.i2b2fact obs
join death_source_map dmap on obs.concept_cd = dmap.concept_cd;

end;
/
BEGIN
PCORNetDeath();
END;
/
update cdm_status
set end_time = sysdate, records = (select count(*) from death)
where task = 'death'
/
select 1 from cdm_status where status = 'death'

--SELECT count(PATID) from death where rownum = 1