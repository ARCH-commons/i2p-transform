whenever sqlerror continue
drop table test_cases;
whenever sqlerror exit;
create table test_cases (
  query_name varchar2(160) not null,
  obs integer not null,
  by_time integer,
  by_value1 varchar2(160),
  by_value2 varchar2(160),
  record_n integer,
  record_pct number,
  distinct_patid_n integer,
  pass integer not null,
  description varchar2(2000)
)
;
comment on column test_cases.query_name is 'a la DRN OS SAS script query name';
comment on column test_cases.obs is 'Obs / line number';
comment on column test_cases.by_time is 'breakdown by year or year/month';
comment on column test_cases.by_value1 is 'relevant nominal value';
comment on column test_cases.by_value2 is '2nd level breakdown';
comment on column test_cases.record_n is 'count our quanitity';
comment on column test_cases.record_pct is 'percent of all observations';
comment on column test_cases.distinct_patid_n is 'patient count';
comment on column test_cases.pass is '1 for pass, 0 for fail';

/* Report how many condition table rows were rejected.
*/
insert into test_cases (query_name, description, pass, obs, record_n, record_pct)
with err as (
  select count(*) qty from pcornet_cdm.ERR$_CONDITION
  ),
total_condition as (
  select count(*) qty from pcornet_cdm.condition
  )
select 'rejected_condition_rows' query_name
     , 'Report how many rows were rejected from the condition table due to table constraints.' description
     , case when err.qty < 1000 then 1 else 0 end pass
     , rownum obs
     , err.qty record_n
     , (err.qty / tc.qty) * 100 record_pct
from total_condition tc cross join err
;


/* "Low bar" test to verify that DISPENSING is not empty */
insert into test_cases (query_name, description, obs, by_value1, by_value2, record_pct, pass)
select
    'DISPENSING' ,
    'Make sure that there are at least 10 dispensing fact' description,
    count(*) obs,
    null by_value1,
    null by_value2,
    null record_pct,
    case when count(*) >= 10 then 1 else 0 end pass
from dispensing
;


/* Test that there are at least a handful of distinct patients associated with
 * DISPENSING facts.
 */
insert into test_cases (query_name, description, obs, by_value1, by_value2, record_pct, pass)
select
  'DISPENSING_PATS',
  'Make sure that there are at least 3 distinct patients with dispensing facts',
  count(distinct dis.patid) obs,
  null by_value1,
    null by_value2,
    null record_pct,
    case when count(distinct dis.patid) >= 3 then 1 else 0 end pass
from dispensing dis
inner join demographic dem
  on dis.patid=dem.patid
;


/* Test that we have some height/weight measurements
*/
insert into test_cases (query_name, description, pass, obs, record_n, record_pct)
with total_vital as (
  select count(*) qty from pcornet_cdm.vital
  ),
ht as (
  select count(*) qty from pcornet_cdm.vital
  where ht is not null
  ),
wt as (
  select count(*) qty from pcornet_cdm.vital
  where wt is not null
  ),
bmi as (
  select count(*) qty from pcornet_cdm.vital
  where original_bmi is not null
  ),
results as (
  select 
    round((ht.qty/total_vital.qty) * 100) pct_ht,
    round((wt.qty/total_vital.qty) * 100) pct_wt,
    round((bmi.qty/total_vital.qty) * 100) pct_bmi
  from total_vital cross join ht cross join wt cross join bmi
  )
select 'some_height_measurements_4335' query_name
     , 'Make sure we have at least some height records' description
     , case when ht.qty > 0 then 1 else 0 end pass
     , rownum obs
     , ht.qty record_n
     , results.pct_ht record_pct
from total_vital cross join ht cross join results
union all
select 'some_weight_measurements_4335' query_name
     , 'Make sure we have at least some weight records' description
     , case when wt.qty > 0 then 1 else 0 end pass
     , rownum obs
     , wt.qty record_n
     , results.pct_wt record_pct
from total_vital cross join wt cross join results
union all
select 'some_bmi_measurements_4335' query_name
     , 'Make sure we have at least some bmi records' description
     , case when bmi.qty > 0 then 1 else 0 end pass
     , rownum obs
     , bmi.qty record_n
     , results.pct_bmi record_pct
from total_vital cross join bmi cross join results
;
commit;

/* Test to make sure we got about the same number of patients in the CDM 
diagnoses that we do in i2b2.
*/
with cdm as (
  select count(distinct patid) qty from diagnosis
  ),
i2b2 as (
  select count(distinct patient_num) qty from i2b2fact 
  where concept_cd in (
    select concept_cd from "&&i2b2_data_schema".concept_dimension 
    where concept_path like '\i2b2\Diagnoses\ICD9\%'
    )
  ),
diff as (
  select ((abs(cdm.qty - i2b2.qty) / i2b2.qty) * 100) pct from cdm cross join i2b2
  )
select case when diff.pct > 10 then 1/0 else 1 end diag_pat_count_ok from diff;


/* Test to make sure we got about the same number of patients in the CDM 
procedure that we do in i2b2.
*/
with cdm as (
  select count(distinct patid) qty from procedures
  ),
i2b2 as (
  select count(distinct patient_num) qty from (
    select * from i2b2fact 
    where concept_cd in (
      select concept_cd from "&&i2b2_data_schema".concept_dimension 
      where concept_path like '\i2b2\Procedures\%'
      )
    )
  ),
diff as (
  select  
    ((abs(cdm.qty - i2b2.qty) / i2b2.qty) * 100) pct 
  from cdm cross join i2b2
  )
select case when diff.pct > 10 then 1/0 else 1 end proc_pat_count_ok from diff;


/* Make sure we have roughly the same number of Hispanic patients in the CDM and
i2b2.
*/
with
num_hispanic_cdm as (
  select count(*) qty from demographic where hispanic = 'Y'
  ),
num_hispanic_i2b2 as (
  select count(*) qty from "&&i2b2_data_schema".patient_dimension where ethnicity_cd = 'Y'
  ),
diff as (
  select ((abs(cdm.qty - i2b2.qty) / i2b2.qty) * 100) pct 
  from num_hispanic_cdm cdm cross join num_hispanic_i2b2 i2b2
  )
select case when diff.pct > 10 then 1/0 else 1 end hisp_y_pat_count_ok from diff;

-- TODO: Consider trying to combine the Y and N tests as they are copy/paste
with
num_hispanic_cdm as (
  select count(*) qty from demographic where hispanic = 'N'
  ),
num_hispanic_i2b2 as (
  select count(*) qty from "&&i2b2_data_schema".patient_dimension where ethnicity_cd = 'N'
  ),
diff as (
  select ((abs(cdm.qty - i2b2.qty) / i2b2.qty) * 100) pct 
  from num_hispanic_cdm cdm cross join num_hispanic_i2b2 i2b2
  )
select case when diff.pct > 10 then 1/0 else 1 end hisp_n_pat_count_ok from diff;

-- Make sure we have some diagnosis source information 
select case when count(*) < 3 then 1/0 else 1 end a_few_dx_sources from (
  select distinct dx_source from diagnosis
  );

-- Make sure we have valid DX_SOURCE values
select case when count(*) > 0 then 1/0 else 1 end valid_dx_sources from (
  select distinct dx_source from diagnosis where dx_source not in ('AD', 'DI', 'FI', 'IN', 'NI', 'UN', 'OT')
  );
  
-- Make sure we have a couple principal diagnoses (PDX)
select case when count(*) < 2 then 1/0 else 1 end a_few_pdx_flags from (
  select distinct pdx from diagnosis
  );

-- Make sure we have valid PDX values
select case when count(*) > 0 then 1/0 else 1 end valid_pdx_flags from (
  select distinct pdx from diagnosis where pdx not in ('P', 'S', 'X', 'NI', 'UN', 'OT')
  );

-- Make sure that there are several different enrollment dates
select case when pct_distinct < 5 then 1/0 else 1 end many_enr_dates from (
  with all_enrs as (
    select count(*) qty from enrollment
    ),
  distinct_date as (
    select count(qty) qty from (
      select distinct enr_start_date qty from enrollment
      )
    )
  select round((distinct_date.qty/all_enrs.qty) * 100, 4) pct_distinct
  from distinct_date cross join all_enrs
  );

-- Make sure most procedure dates are not null
select case when pct_not_null < 99 then 1/0 else 1 end some_px_dates_not_null from (
  with all_px as (
    select count(*) qty from procedures
    ),
  not_null as (
    select count(*) qty from procedures where px_date is not null
    )
  select round((not_null.qty / all_px.qty) * 100, 4) pct_not_null 
  from not_null cross join all_px
);

-- Make sure we have some procedure sources (px_source)
select case when count(*) = 0 then 1/0 else 1 end have_px_sources from (
  select distinct px_source from procedures where px_source is not null
  );

-- Make sure we have some encounter types
select case when pct_known < 20 then 1/0 else 1 end some_known_enc_types from (
  with all_enc as (
    select count(*) qty from encounter
    ),
  known_enc as (
    select count(*) qty from encounter where enc_type is not null and enc_type != 'UN'
    )
  select round((known_enc.qty / all_enc.qty) * 100, 4) pct_known 
  from known_enc cross join all_enc
  );
  

insert into test_cases (query_name, description, pass
                      , obs, record_n, record_pct, by_time, by_value1)
select 'ENC_L3_ENCTYPE_ADATE_YM' query_name
     , 'TODO: chase down ~80% unknown enc_type' description
     , case when enc_type in ('UN', 'OT') and pct < 25 then 1
            when enc_type not in ('UN', 'OT') and freq > 1 then 1
            else 0 end pass
     , rownum obs
     , t.*
from (
with enc as (
  select extract (year from admit_date) * 100 +
         extract (month from admit_date) admit_ym
       , enc_type
  from encounter
), enc_agg as (
  select count(*) qty from encounter
), by_time as (
  select count(*) qty, admit_ym
  from enc group by admit_ym
), by_val as (
  select count(*) freq
       , round(count(*) / enc_agg.qty * 100, 1) pct
       , admit_ym
       , enc_type
  from enc cross join enc_agg
  group by admit_ym, enc_type, enc_agg.qty
  order by admit_ym, enc_type)
select by_val.freq
     , round(by_val.freq / by_time.qty * 100, 4) pct
     , by_val.admit_ym
     , by_val.enc_type
from by_time join by_val on by_time.admit_ym = by_val.admit_ym
) t
;


insert into test_cases (query_name, description, pass
                      , obs, by_value1, by_value2, record_n, record_pct, distinct_patid_n)
select 'VIT_L3_SMOKING' query_name
     , 'make sure we have something about patient smoking tobacco use' description
     , case
         when smoking = 'NI' and record_pct < 95 then 1
         when smoking != 'NI' and distinct_patid_n > 2 then 1
         else 0
       end pass
     , rownum obs
     , q.*
from (
with smoking_terms as (
select distinct pcori_basecode
from pcornet_vital
where c_fullname like '\PCORI\VITAL\TOBACCO\SMOKING\%'
),
vit as (
select patid
     , smoking
     , case when smoking = 'NI' then null
         when exists (
         select pcori_basecode
         from smoking_terms
         where smoking_terms.pcori_basecode = vital.smoking)
         then null
         else 1
         end value_outside
from vital
),
agg as (
  select count(*) tot from vital)
select smoking, value_outside
     , count(*)
     , round(count(*) / tot * 100, 4) record_pct
     , count(distinct patid) distinct_patid_n
from vit cross join agg
group by smoking, value_outside, tot
order by smoking
) q
;


/* Test to make sure we have something about patient non-smoking tobacco use */
insert into test_cases (query_name, description, pass
                      , obs, by_value1, by_value2, record_n, record_pct, distinct_patid_n)
	select 'VIT_L3_TOBACCO' query_name
	     , 'make sure we have something about patient non-smoking tobacco use' description
	     , case
	         when tobacco = 'NI' and record_pct < 95 then 1
	         when tobacco != 'NI' and distinct_patid_n > 2 then 1
	         else 0
	       end pass
	     , rownum obs
	     , q.*
	from (
	with tobacco_terms as (
	select distinct pcori_basecode
	from pcornet_vital
	where c_fullname like '\PCORI\VITAL\TOBACCO\02\%'
	),
	vit as (
	select patid
	     , tobacco
	     , case when tobacco = 'NI' then null
	         when exists (
	         select pcori_basecode
	         from tobacco_terms
	         where tobacco_terms.pcori_basecode = vital.tobacco)
	         then null
	         else 1
	         end value_outside
	from vital
	),
	agg as (
	  select count(*) tot from vital)
	select tobacco, value_outside
	     , count(*)
	     , round(count(*) / tot * 100, 4) record_pct
	     , count(distinct patid) distinct_patid_n
	from vit cross join agg
	group by tobacco, value_outside, tot
	order by tobacco
	) q
	;


/* Test to make sure we have something about tobacco use types */
insert into test_cases (query_name, description, pass
                      , obs, by_value1, by_value2, record_n, record_pct, distinct_patid_n)
	select 'VIT_L3_TOBACCO_TYPE' query_name
	     , 'make sure we have something about patient tobacco use types' description
	     , case
	         when tobacco_type = 'NI' and record_pct < 95 then 1
	         when tobacco_type != 'NI' and distinct_patid_n > 2 then 1
	         else 0
	       end pass
	     , rownum obs
	     , q.*
	from (
	with tobacco_type_terms as (
    select '01' pcori_basecode from dual
    union all
    select '02' pcori_basecode from dual
    union all
    select '03' pcori_basecode from dual
    union all
    select '04' pcori_basecode from dual
    union all
    select '05' pcori_basecode from dual
    union all
    select '06' pcori_basecode from dual
    union all
    select 'NI' pcori_basecode from dual
    union all
    select 'UN' pcori_basecode from dual
    union all
    select 'OT' pcori_basecode from dual
	),
	vit as (
	select patid
	     , tobacco_type
	     , case when tobacco_type = 'NI' then null
	         when exists (
	         select pcori_basecode
	         from tobacco_type_terms
	         where tobacco_type_terms.pcori_basecode = vital.tobacco_type)
	         then null
	         else 1
	         end value_outside
	from vital
	),
	agg as (
	  select count(*) tot from vital)
	select tobacco_type, value_outside
	     , count(*)
	     , round(count(*) / tot * 100, 4) record_pct
	     , count(distinct patid) distinct_patid_n
	from vit cross join agg
	group by tobacco_type, value_outside, tot
	order by tobacco_type
	) q
	;


insert into test_cases (query_name, description, pass, obs, by_value1, record_n, record_pct)
select 'ENC_L3_DRG' query_name
     , 'make sure we have some MS-DRGs' description
     , case when drg  = 'NULL or missing' and record_pct < 95 then 1
            when drg != 'NULL or missing' and record_n >= 1 then 1
            else 0
       end pass
     , rownum obs
     , q.*
from (
with enc as (
select coalesce(drg, 'NULL or missing') drg
from encounter
),
agg as (
  select count(*) tot from encounter)
select drg, count(*) record_n, round(count(*) / agg.tot * 100, 4) record_pct
from enc cross join agg
group by drg, agg.tot
order by 2 desc
) q
;


insert into test_cases (query_name, description, pass, obs, by_value1, record_n, record_pct)
select 'ENC_L3_N' query_name
     , 'providerid: many distinct' description
     , case when distinct_n / all_n * 100 >= 0.01
             and null_n / all_n * 100 < 40 then 1
            else 0 end pass
     , rownum obs
     , t.tag, t.distinct_n, round(t.distinct_n / t.all_n * 100, 4)
from (
with enc as (
select encounterid
     , case when encounterid is null then 1 else null end encounterid_null
     , patid
     , case when patid is null then 1 else null end patid_null
     , providerid
     , case when providerid is null then 1 else null end providerid_null
from encounter
)
select 'encounterid' tag
     , count(encounterid) all_n, count(distinct encounterid) distinct_n, count(encounterid_null) null_n
from enc
union all
select 'patid' tag
     , count(patid), count(distinct patid), count(patid_null)
from enc
union all
select 'providerid' tag
     , count(providerid), count(distinct providerid), count(providerid_null)
from enc
) t
;


insert into test_cases (query_name, description, pass, obs, by_value1, by_time, record_n, record_pct, distinct_patid_n)
select 'ENC_L3_ENCTYPE_DDATE_YM' query_name
     , '
"Discharge date. Should be populated for all
Inpatient Hospital Stay (IP) and Non-Acute
Institutional Stay (IS) encounter types."
 -- PCORnet CDM v3
' description
     , case when enc_type not in ('IP', 'IS') then 1
            when discharge_date is not null then 1
            when record_pct < 10 then 1
            else 0
       end pass
     , q.*
from (
  select rownum obs, q.* from (
    with enc_agg as (
      select count(*) tot from encounter
    ),
    enc as (
      select extract(year from discharge_date) * 100 +
             extract(month from discharge_date) discharge_date
           , encounterid
           , patid
           , enc_type
           , case when discharge_date is null
                    or discharge_date = 'NI'
             then 1 else 0 end discharge_date_null
      from encounter )
    select enc_type
         , discharge_date
         , count(*) record_n
         , round(count(*) / enc_agg.tot * 100, 4) record_pct
         , count(distinct patid) distinct_patid_n
    from enc cross join enc_agg
    group by discharge_date, enc_type, enc_agg.tot
    order by 3 desc) q
    ) q
where q.obs <= 100;


insert into test_cases (query_name, description, pass, obs, by_value1, record_n, record_pct)
select 'ENC_L3_DISDISP' query_name
     , 'too many NI' description
     , case when discharge_disposition = 'NI' and record_pct < 40 then 1
            when discharge_disposition is null and record_pct < 5 then 1
            when discharge_disposition != 'NI' then 1
            else 0
       end pass
     , rownum obs
     , q.*
from (
with enc_agg as (
  select count(*) tot from encounter)
select discharge_disposition
     , count(*) record_n
     , round(count(*) / enc_agg.tot * 100, 4) record_pct
from encounter cross join enc_agg
group by discharge_disposition, enc_agg.tot
order by 1
) q
;


insert into test_cases (query_name, description, pass, obs, by_value1, record_n, record_pct)
select 'ENC_L3_DISSTAT' query_name
     , 'too many NI' description
     , case when discharge_status = 'NI' and record_pct < 40 then 1
            when discharge_status is null and record_pct < 5 then 1
            when discharge_status != 'NI' then 1
            else 0
       end pass
     , rownum obs
     , q.*
from (
with enc_agg as (
  select count(*) tot from encounter)
select discharge_status
     , count(*) record_n
     , round(count(*) / enc_agg.tot * 100, 4) record_pct
from encounter cross join enc_agg
group by discharge_status, enc_agg.tot
order by 1
) q
;


/*  Should be populated for Inpatient Hospital Stay (IP) and Non-Acute 
Institutional Stay (IS) encounter types.
*/
insert into test_cases (query_name, description, pass, obs, by_value1, record_n, record_pct)
select 'ENC_L3_ADMSRC' query_name
     , 'too many NI' description
     , case when admitting_source = 'NI' and record_pct < 40 then 1
            when admitting_source is null and record_pct < 5 then 1
            when admitting_source != 'NI' then 1
            else 0
       end pass
     , rownum obs
     , q.*
from (
  with ipis as (
    select * from encounter where enc_type in ('IP', 'IS')
    ),
  enc_agg as (
    select count(*) tot from ipis
    )
  select admitting_source
       , count(*) record_n
       , round(count(*) / enc_agg.tot * 100, 4) record_pct
  from ipis cross join enc_agg
  group by admitting_source, enc_agg.tot
  order by 1
  ) q
;

/* Due to using hostpital accounts as encounters,
we have a long tail of very long encounters; hundreds of days.

select count(*), los from (
select round(discharge_date - admit_date) LOS
from encounter
where enc_type in ('IP', 'IS')
) group by los
order by 2
;
*/


/*  Verify that LAB_RESULT_CM contains data associated with a least half of the 
labs defined in PMN_LABLOCAL.
*/
insert into test_cases (query_name, description, obs, by_value1, by_value2, record_pct, pass)
with partial_lab_counts as (
  select lab_name, count(lab_name) lab_count
  from lab_result_cm
  group by lab_name
), complete_lab_counts as(
  select replace(pmn.lab_name, 'LAB_NAME:', '') lab_name,
    case when lc.lab_count is null then 0 else lc.lab_count end lab_count
  from pmn_labnormal pmn
  left outer join partial_lab_counts lc on concat('LAB_NAME:', lc.lab_name) = pmn.lab_name
), total_lab_types as (
  select count(*) num_labs from complete_lab_counts
), present_lab_types as (
  select count(*) num_labs from complete_lab_counts
  where lab_count > 0
)
select
'LAB_RESULT_CM Test' query_name,
'LAB_RESULT_CM contains data associated with at least half of the defined labs' description,
rownum obs,
plt.num_labs by_value1,
tlt.num_labs by_values2,
round(plt.num_labs/tlt.num_labs, 4) record_pct,
case when (plt.num_labs/tlt.num_labs) > 0.5 then 1 else 0 end pass
from present_lab_types plt
cross join total_lab_types tlt
;

select case when count(*) > 0 then 1/0 else 1 end all_test_cases_pass from (
select * from test_cases where pass = 0
);

