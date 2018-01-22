/** epic_flowsheets_transform.sql -- prepare to load Epic EMR flowsheet facts

Copyright (c) 2012 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

 * See also epic_flowsheets_multiselect.sql, epic_etl.py, epic_facts_load.sql
 *
 * References:
 *
 * Lesson 4: Flowsheet Data
 * Clarity Data Model - EpicCare Inpatient Spring 2008 Training Companion
 * https://userweb.epic.com/epiclib/epicdoc/EpicWiseSpr08/Clarity/Clarity%20Training%20Companions/CLR209%20Data%20Model%20-%20EpicCare%20Inpatient/04TC%20Flowsheet%20Data.doc
 */

select test_name from etl_tests where 'dep' = 'etl_tests_init.sql';

-- Check that this is the ID instance
select flo_meas_id from CLARITY.ip_flwsht_meas where 1=0;

-- to see the time detail...
alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH:MI';

-- define heron_obs_sample = sample (0.001, 1234)

create or replace view etl_test_domain_flowsheets as
select 'Flowsheets' test_domain from dual;

insert into etl_test_values (test_value, test_domain, test_name, result_id, result_date) 
with test_key as (
  select test_domain,
  'val_type_seen_before' test_name from etl_test_domain_flowsheets
  )
, test_values as (
  select count(*) test_value, test_key.* from (
    select distinct zcvt.name
    from CLARITY.ip_flwsht_meas ifm
    join CLARITY.ip_flo_gp_data ifgd
      on ifm.flo_meas_id= ifgd.flo_meas_id
    join CLARITY.zc_val_type zcvt on ifgd.val_type_c = zcvt.val_type_c
    where zcvt.name not in (
      'Blood Pressure', 'Category Type', 'Concentration', 'Custom List',
      'Date', 'Dose', 'Height', 'Numeric Type', 'Patient Height', 
      'Patient Weight', 'Rate', 'String Type', 'Temperature', 'Time', 'Weight')
    ), test_key
  )
select test_value, test_domain, test_name, sq_result_id.nextval, sysdate
from test_values
;


/***************
 * flo_meas_type -- per-flo_meas_id transformation

Starting with relevant columns from ip_flo_gp_data,
determine i2b2 valtype_cd, UNITS_CD.

See also i2b2_facts_deid.sql regarding identified/de-identified data
flag in valtype_cd.

Values for height, weight, and temperature are (for reasons lost to history)
converted to SI units.

Also, trivially set up MODIFIER_CD, CONFIDENCE_NUM.

TODO: consider migrating to CSV spreadsheet. Too bad SQL doesn't
      have relation constants a la R read.table() or JSON.

 */
-- compute i2b2 valtype_cd for each flo_meas_id
create or replace view flo_meas_type as
select ifgd.flo_meas_id, ifgd.disp_name
     , ifgd.multi_sel_yn
     , ifgd.record_state_c
     , zcvt.name val_type
     , case
       when zcvt.name in (
         'Numeric Type',
         'Blood Pressure', 'Temperature', 'Time',
         'Patient Weight', 'Weight',
         'Patient Height', 'Height') then 'N'
       when zcvt.name in ('Custom List', 'Category Type') then '@'
         -- only add ID information to nightheron
       when zcvt.name in ('String Type') then 'Ti'
       when zcvt.name in ('Date') then 'D'
       else null  -- val_type_seen_before test keeps these bounded
       end valtype_cd
     , 'KUH|FLO_MEAS_ID:' || ifgd.flo_meas_id CONCEPT_CD

     -- Convert to SI units per 2011 design.
     , case
       when zcvt.name in (
       /* Epic documentation doesn't give units for 'Weight',
          so we've determined emperically that it's oz.
          See also mean Birth Weight test based on flo_stats_num
          in epic_flowsheets_multiselect.sql */
         'Patient Weight', 'Weight') then 1.0 / 16.0 / 2.2  -- 16 oz/lb; 2.2kg/lb       
       when zcvt.name in (
         'Patient Height', 'Height') then 2.54  -- 2.54 cm/in
       when zcvt.name in (
         'Temperature') then 5/9  -- F to C
       else 1
       end scale
     , case
       when zcvt.name in (
         'Temperature') then -32  -- F to C
       else 0
       end bias
     , case
       when zcvt.name in (
         'Patient Weight', 'Weight')  then 'kg'
       when zcvt.name in (
         'Temperature') then 'C'
       when zcvt.name in (
         'Time') then 's'
       when zcvt.name in (
         'Patient Height', 'Height') then 'cm'
       when zcvt.name in (
         'Blood Pressure') then 'mmHg'
       else ifgd.units
       end UNITS_CD
     , '@'  MODIFIER_CD
     , to_number(null) CONFIDENCE_NUM
from CLARITY.ip_flo_gp_data ifgd
left join CLARITY.zc_val_type zcvt on ifgd.val_type_c = zcvt.val_type_c
;


/***************
 * flowsheet_day -- per-patient-day transformation

Starting with ip_flwsht_rec, compute ENCOUNTER_IDE, PATIENT_IDE
(to be mixed with patient_mapping and encounter_mapping in epic_facts_load.sql).

Trivially set LOCATION_CD.

 */
create or replace view flowsheet_day as
select ifr.record_date
     , ifr.pat_id
     , ifr.fsd_id
     , ifr.INPATIENT_DATA_ID
     -- i2b2 equivalents common to many/all datatypes

     -- patient day; see patient_day_visit view
     ,  TO_CHAR(ifr.record_date,'YYYYMMDD') || ifr.pat_id
        ENCOUNTER_IDE
     , ifr.pat_id PATIENT_IDE
     , '@' LOCATION_CD
from CLARITY.ip_flwsht_rec ifr
;


/***************
 * flowsheet_obs -- per-measurement transformation common to all datatypes

Starting with ip_flwsht_meas, compute instance_num,
start_date, end_date, update_date.

Filter out rows where meas_value is null.

Trivially (for now) set PROVIDER_ID, valueflag_cd.

 */
create or replace view flowsheet_obs as
select /*+ parallel(16) */ -- ISSUE: parameterize parallel_degree?
       ifm.fsd_id
     , ifm.line
     , ifm.flo_meas_id
     , ifm.meas_value
     , ifm.recorded_time
     , ifm.entry_time
     -- per-measurement conversion to i2b2 terms
     , '@' PROVIDER_ID -- todo: use ifm.TAKEN_USER_ID
     , recorded_time START_DATE
     , ifm.fsd_id * 100000 + ifm.line instance_num
     , '@' valueflag_cd -- TODO #3850: [H]igh/[L]ow/[A]bnormal based on ifm.ABNORMAL_C
     , recorded_time END_DATE
     , entry_time UPDATE_DATE
from CLARITY.ip_flwsht_meas ifm
where meas_value is not null
;


/********************
 * flowsheet_num_data - handle number syntax

Starting with flowsheet_obs, split diastolic and systolic into separate rows,
flag (but don't filter) illegal numerals, and convert the rest to_number().

Values such as 2.22222222222222E+22 don't fit in nval_num,
which is declared NUMBER(18,5). Let's throw out values using E notation.
The `numerals_filtered` test (in epic_flowsheets_multiselect) verifies that
we're not throwing away more than a handful.

Stats in epic_flowsheets_multiselect.sql are based on this view,
before we join with per-patient-day or per-flow-measure info.

 */
create or replace view flowsheet_num_data as
with
parts as (
  select flo_meas_id, bp_part, pattern
  from flo_meas_type fmt
  left join (  
          select 'Blood Pressure' val_type, '_DIASTOLIC' bp_part, '\d+$' pattern from dual
union all select 'Blood Pressure' val_type, '_SYSTOLIC' bp_part, '^\d+' pattern from dual
  ) parts on parts.val_type = fmt.val_type
  where fmt.valtype_cd like 'N%'
)
, numerals as (
  select fsd_id, line
       , obs.flo_meas_id
       , bp_part
       , meas_value
       , recorded_time
       , entry_time
       , case
         when parts.pattern is not null
         then regexp_substr(meas_value, parts.pattern)
         else meas_value
         end numeral
       , START_DATE
       , END_DATE
       , instance_num
       , UPDATE_DATE
       , PROVIDER_ID
       , valueflag_cd
  from flowsheet_obs obs
  join parts on parts.flo_meas_id = obs.flo_meas_id
)
, num_check as (
  select numerals.*
       , case
         when numeral is null then null
         when regexp_like(numeral, '^-?\d*(\.\d+)?$') then 1
         else 0
         end ok
  from numerals
)
  select num_check.*
       , case
         when ok = 1 then to_number(numeral)
         else null
         end meas_value_num
  from num_check
;


/**************
 * numerictypeflows - Numeric, Blood Pressure, Weight, etc.

Build concept_cd using blood pressure suffix; apply units bias and scale;
trivially set TVAL_CHAR.

Join flowsheet_num_data with flo_meas_type and flowsheet_day to fill in
remaining observation_fact style fields.

 */
create or replace view numerictypeflows as
select ENCOUNTER_IDE
     , PATIENT_IDE
     , 'KUH|FLO_MEAS_ID:' || num_data.flo_meas_id || bp_part concept_cd
     , PROVIDER_ID
     , START_DATE
     , MODIFIER_CD
     , instance_num
     , valtype_cd
     , 'E' TVAL_CHAR -- i2b2 doc says 'EQ' but demo data says 'E'
     , case when fmt.bias <> 0 or fmt.scale <> 1
       then
         -- Round so as not to imply more precision than was measured.
         round((meas_value_num + fmt.bias) * fmt.scale, 3)
       else
         meas_value_num
       end nval_num
     , valueflag_cd
     , UNITS_CD
     , END_DATE
     , LOCATION_CD
     , CONFIDENCE_NUM
     , UPDATE_DATE
from flowsheet_num_data num_data
join flo_meas_type fmt on fmt.flo_meas_id = num_data.flo_meas_id
join flowsheet_day fsd on fsd.fsd_id = num_data.fsd_id
where num_data.ok = 1
;

-- select * from numerictypeflows;

  
/***********
 * datemeasureflows - for value_type_name "Date"
 * 
 * STRANGE! Syntax is EITHER:
 *  - a number of days since Dec 31, 1840 or
 *  - a date in 'MM/DD/YYYY' format
 */
create or replace view datemeasureflows as
Select
      ENCOUNTER_IDE
    , PATIENT_IDE
    , CONCEPT_CD
    , PROVIDER_ID
    , START_DATE
    , MODIFIER_CD
    , instance_num
    , 'D' VALTYPE_CD,  
      case 
          when substr(meas_value , 1, instr(meas_value , '/') -1 ) is NULL
          then to_char(to_date('12/31/1840', 'MM/DD/YYYY') + meas_value, 'YYYY-MM-DD')
          else to_char(to_date(meas_value, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      end TVAL_CHAR
    , null NVAL_NUM
    , valueflag_cd
    , UNITS_CD
    , END_DATE
    , LOCATION_CD
    , CONFIDENCE_NUM
    , UPDATE_DATE 
from  flowsheet_obs obs
join flo_meas_type fmt on fmt.flo_meas_id = obs.flo_meas_id
join flowsheet_day fsd on fsd.fsd_id = obs.fsd_id
where valtype_cd like 'D%';


/* todo:: test date-shifting when VALUE_TYPE_NAME='Date'
        e.g. 11568 or 700 PLACEMENT DATE (#299) */


/*****************
 * idstringtypeflows -- free text

For measures identifed (in flo_meas_type) as free text, set tval_char.

Trivially set nval_num.
 */
create or replace view idstringtypeflows as
select
      fsd.ENCOUNTER_IDE
    , fsd.PATIENT_IDE
    , fmt.CONCEPT_CD
    , obs.PROVIDER_ID
    , obs.START_DATE
    , fmt.MODIFIER_CD
    , obs.instance_num
    , fmt.VALTYPE_CD
    , obs.meas_value TVAL_CHAR
    , to_number(null) NVAL_NUM
    , obs.valueflag_cd
    , fmt.UNITS_CD
    , obs.END_DATE
    , fsd.LOCATION_CD
    , fmt.CONFIDENCE_NUM
    , obs.UPDATE_DATE
from  flowsheet_obs obs
join flo_meas_type fmt on fmt.flo_meas_id = obs.flo_meas_id
join flowsheet_day fsd on fsd.fsd_id = obs.fsd_id
where valtype_cd like 'T%';


/*****************
 * deidstringtypeflows -- deid-ed free text

For measures identifed (in flo_meas_type) as free text, and have corresponding
de-identified meas_values.

As an intermediate step create `approved_deid_flowsheets` containing all of the
de-identified free text notes that have been approved for inclusion.

Trivially set nval_num.
 */
 

whenever sqlerror continue;
drop table approved_deid_flowsheets;
whenever sqlerror exit;


create table approved_deid_flowsheets (
       fsd_id number
     , line   number 
     , flo_meas_id number
     , meas_value varchar(4000)
     , primary key (fsd_id, line));


whenever sqlerror continue;
drop table approved_flo_meas_ids;
-- Oracle gave really bad performance while `deidentify.deid_flowsheet_approval`
-- was a view.
create table approved_flo_meas_ids as
select * from deidentify.deid_flowsheet_approval where approved_to_deid = 1;

insert into approved_deid_flowsheets
(fsd_id, line, flo_meas_id, meas_value)
select fsd_id, line, flo_meas_id, meas_value
from (
      -- `fsd_id` and `line` are extracted from `flo_notes.id`
      -- see: heron_staging/deidentify/Flowsheets-DEID-Stage.sql
      select to_number(substr(to_char(id), 
                              1, 
                              length(to_char(id))-5)) as fsd_id
           , to_number(substr(id, -5)) as line
           , to_number(note_id) as flo_meas_id
           , deid_note_text as meas_value
      from deidentify.flo_notes) deid
      -- Include only meas_values that have flo_meas_id approved.
where exists (select * from approved_flo_meas_ids af
              where deid.flo_meas_id = af.flo_meas_id
             );
whenever sqlerror exit;


insert into etl_test_values (test_value, test_domain, test_name, result_id, 
                             result_date) 
with test_key as (
  select test_domain,
  'staged_deid_flowsheet' test_name from etl_test_domain_flowsheets
  )
, test_values as (
  select count(*) test_value, test_key.* from approved_deid_flowsheets, test_key
  )
select test_value, test_domain, test_name, sq_result_id.nextval, sysdate
from test_values
;

-- Did `staged_deid_flowsheet` fail?  First check to see if the necessary
-- staged tables exist.

-- select * from deidentify.flo_notes;
-- select * from deidentify.deid_flowsheet_approval;

create or replace view deidstringtypeflows as
select
      fsd.ENCOUNTER_IDE
    , fsd.PATIENT_IDE
    , fmt.CONCEPT_CD
    , obs.PROVIDER_ID
    , obs.START_DATE
    , fmt.MODIFIER_CD
    , obs.instance_num
    , 'Td' as valtype_cd
    , deid.meas_value TVAL_CHAR
    , to_number(null) NVAL_NUM
    , obs.valueflag_cd
    , fmt.UNITS_CD
    , obs.END_DATE
    , fsd.LOCATION_CD
    , fmt.CONFIDENCE_NUM
    , obs.UPDATE_DATE
from  flowsheet_obs obs
join flo_meas_type fmt on fmt.flo_meas_id = obs.flo_meas_id
join flowsheet_day fsd on fsd.fsd_id = obs.fsd_id
join approved_deid_flowsheets deid
  on deid.fsd_id = obs.fsd_id
 and deid.line = obs.line
where valtype_cd like 'T%';


/********************
 * flowsheet_nom_data - nominal data, including multi select

Stats in epic_flowsheets_multiselect.sql are based on this view,
before we join with per-patient-day or per-flow-measure info.

The flow_measure_multi table (eventually) maps 'x;y;z' multi-select values
to the constituent 'x', 'y', and 'z' values. Create the table so we can
refer to it, but leave populating it to epic_flowsheets_multiselect.sql.

 */
whenever sqlerror continue;
drop table flow_measure_multi;
whenever sqlerror exit;
create table flow_measure_multi as
select fm.meas_value
     , 0 val_ix
     , fm.meas_value choice_label
from flowsheet_obs fm
where 1=0;

create or replace view flowsheet_nom_data as
  select fsd_id, obs.line
       , obs.flo_meas_id
       , fmt.multi_sel_yn
       , obs.meas_value
       , fmm.val_ix
       , fmm.choice_label multi_choice_label
       , coalesce(fmm.choice_label, obs.meas_value) choice_label
       , recorded_time
       , entry_time
       , START_DATE
       , END_DATE
       , UPDATE_DATE
       , PROVIDER_ID
       , valueflag_cd
  from flowsheet_obs obs
  join flo_meas_type fmt on fmt.flo_meas_id = obs.flo_meas_id
       and fmt.valtype_cd = '@'  -- valtype_cd for nominal data
  left join flow_measure_multi fmm
    -- Try not to look in flow_measure_multi unless
    -- except when we have multi_sel_yn and a ';' .
    on fmm.meas_value = case
       when fmt.multi_sel_yn = '1'
        and obs.meas_value like '%;%'
       then obs.meas_value
       else null
       end
;

-- "forward reference" flo_stats_nom table for use in flowsheets_concepts.sql
whenever sqlerror continue;
drop table flo_stats_nom;
whenever sqlerror exit;

create table flo_stats_nom as
select flo_meas_id, meas_value choice_label
     , 0 qty
     , 0 tot
     , 99.9 pct
     , date '2001-01-01' recorded_time_min
     , date '2001-01-01' recorded_time_max
from flowsheet_nom_data
where 1 = 0
;


/*************
 * selectflows -- Custom List nominal observations

Compute concept_cd using flo_meas_id and ip_flo_cust_list.line where available;
fall back to hash of choice label.

Trivially set tval_char, nval_num.
 */

create or replace view selectflows as 
Select ENCOUNTER_IDE
     , PATIENT_IDE
     , (case when ifcl.line is not null
        then 'KUH|FLO_MEAS_ID+LINE:' || obs.flo_meas_id || '_' || ifcl.line
        else 'KUH|FLO_MEAS_ID+hash:' || obs.flo_meas_id || '_' || ora_hash(obs.choice_label)
        end)  CONCEPT_CD
     , PROVIDER_ID
     , START_DATE
     , MODIFIER_CD
       -- Handle cases such as 'Sinus Tachycardia;ST'
       -- where multiple choices map to the same ifcl.line
       -- by including the val_ix (1, 2) in instance_num.
     , (obs.fsd_id * 100000 + obs.line) * 10 + coalesce(val_ix, 0) instance_num
     , VALTYPE_CD
     , '@' TVAL_CHAR
     , to_number(null) NVAL_NUM
     , valueflag_cd
     , UNITS_CD
     , END_DATE
     , LOCATION_CD
     , CONFIDENCE_NUM
     , UPDATE_DATE
from  flowsheet_nom_data obs
join flo_meas_type fmt on fmt.flo_meas_id = obs.flo_meas_id
join flowsheet_day fsd on fsd.fsd_id = obs.fsd_id
left join CLARITY.ip_flo_cust_list ifcl
  on ifcl.flo_meas_id = obs.flo_meas_id
 and (obs.choice_label = ifcl.abbreviation or
      obs.choice_label = ifcl.custom_value)
where valtype_cd = '@';

-- select * from multiselectflows where rownum < 200;


/* complete check */
create or replace view epic_flowsheets_txform_sql as
select &&design_digest design_digest from dual;

select 1 up_to_date
from epic_flowsheets_txform_sql where design_digest = &&design_digest;
