select 'DEMOGRAPHIC' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", 'N/A' "Distinct Encounters" from DEMOGRAPHIC
 union all
select 'ENROLLMENT' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", 'N/A' "Distinct Encounters" from ENROLLMENT
 union all
select 'ENCOUNTER' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", to_char(count(distinct encounterid)) "Distinct Encounters" from ENCOUNTER
 union all
select 'DIAGNOSIS' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", to_char(count(distinct encounterid)) "Distinct Encounters" from DIAGNOSIS
 union all
select 'PROCEDURES' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", to_char(count(distinct encounterid)) "Distinct Encounters" from PROCEDURES
 union all
select 'VITAL' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", to_char(count(distinct encounterid)) "Distinct Encounters" from VITAL
 union all
select 'DISPENSING' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", 'N/A' "Distinct Encounters" from DISPENSING
 union all
select 'LAB_RESULT_CM' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", to_char(count(distinct encounterid)) "Distinct Encounters" from LAB_RESULT_CM
 union all
select 'CONDITION' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", to_char(count(distinct encounterid)) "Distinct Encounters" from CONDITION
 union all
select 'PRO_CM' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", to_char(count(distinct encounterid)) "Distinct Encounters" from PRO_CM
 union all
select 'PRESCRIBING' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", to_char(count(distinct encounterid)) "Distinct Encounters" from PRESCRIBING
 union all
select 'PCORNET_TRIAL' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", 'N/A' "Distinct Encounters" from PCORNET_TRIAL
 union all
select 'DEATH' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", 'N/A' "Distinct Encounters" from DEATH
 union all
select 'DEATH_CAUSE' "Table", to_char(count(*)) "Total Rows", to_char(count(distinct patid)) "Distinct Patients", 'N/A' "Distinct Encounters" from DEATH_CAUSE
 union all
select 'HARVEST' "Table", to_char(count(*)) "Total Rows", 'N/A' "Distinct Patients", 'N/A' "Distinct Encounters" from HARVEST
;

select sum("Size (MB)") "Total size, all tables (MB)" from (
  select 
    segment_name "Table", bytes/1024/1024 "Size (MB)"
  from dba_segments
  where owner = '&&pcornet_cdm_user'
  and segment_type = 'TABLE'
  and segment_name in ('DEMOGRAPHIC', 'ENROLLMENT', 'ENCOUNTER', 'DIAGNOSIS', 
    'PROCEDURES', 'VITAL', 'DISPENSING', 'LAB_RESULT_CM', 'CONDITION', 'PRO_CM', 
    'PRESCRIBING', 'PCORNET_TRIAL', 'DEATH', 'DEATH_CAUSE', 'HARVEST')
  );

SELECT sum(bytes/1024/1024/1024) "Total schema size (GB)"
  FROM dba_segments
 WHERE owner = '&&pcornet_cdm_user';
