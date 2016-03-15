whenever sqlerror continue
drop table test_cases;
whenever sqlerror exit;
create table test_cases (
  pass integer,
  query_name varchar2(160),
  value varchar2(160),
  freq number,
  pct number,
  description varchar2(2000)
)
;
comment on column test_cases.pass is '1 for pass, 0 for fail';
comment on column test_cases.query_name is 'a la DRN OS SAS script query name';
comment on column test_cases.value is 'relevant nominal or scalar value';
comment on column test_cases.freq is 'frequency; i.e. count our quanitity';
comment on column test_cases.pct is 'percent of all observations';


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
  
/** TODO: chase down ~80% unknown enc_type */
/*
with enc_agg as (
select count(*) qty from encounter)
select count(*) encounter_qty
     , round(count(*) / enc_agg.qty * 100, 1) pct
     , enc_type
from encounter enc
cross join enc_agg
where exists (
  select admit_date from diagnosis dx where enc.patid = dx.patid)
or exists (
  select admit_date from procedures px where enc.patid = px.patid)
group by enc_type, enc_agg.qty
;
*/

/* Test to make sure we have something about patient smoking tobacco use */
with snums as (
  select smoking cat, count(smoking) qty from vital group by smoking
),
tot as (
  select sum(qty) as cnt from snums
),
calc as (
  select snums.cat, (snums.qty/tot.cnt*100) pct,
    case when (snums.qty/tot.cnt*100) > 1 then 1 else 0 end tst
  from snums, tot 
  where snums.cat!='NI'
)
select case when sum(calc.tst) < 3 then 1/0 else 1 end smoking_count_ok from calc;


/* Test to make sure we have something about patient general tobacco use */
with tnums as (
  select tobacco cat, count(tobacco) qty from vital group by tobacco
),
tot as (
  select sum(qty) as cnt from tnums
),
calc as (
  select tnums.cat, (tnums.qty/tot.cnt*100) pct, case when (tnums.qty/tot.cnt*100) > 1 then 1 else 0 end tst from tnums, tot where tnums.cat!='NI'
)
select case when sum(calc.tst) < 3 then 1/0 else 1 end pass from calc;


/* Test to make sure we have something about tobacco use types */
with ttnums as (
  select tobacco_type cat, count(tobacco) qty from vital group by tobacco_type order by cat
),
tot as (
  select sum(qty) as cnt from ttnums
),
calc as (
  select ttnums.cat, (ttnums.qty/tot.cnt*100) pct, case when (ttnums.qty/tot.cnt*100) > 1 then 1 else 0 end tst from ttnums, tot where ttnums.cat!='NI'
)
select case when sum(calc.tst) < 2 then 1/0 else 1 end pass from calc;


insert into test_cases (query_name, description, pass, freq, pct, value)
select 'some_providers_not_null' query_name
     , 'Make sure most provider ids in the visit dimension are
        not null/unknown/no information' description
     , case when value = 'OK' and pct > 70 then 1
            when value = 'UN' and pct < 30 then 1
            else 0 end pass
     , t.*
from (
  with all_enc as (
    select count(*) qty from encounter),
  provider_ok as (
    select case when providerid is not null
                and providerid not in ('NI', 'UN') then 'OK'
           else 'UN'
           end value
    from encounter)
  select count(*) freq, round(count(*) / all_enc.qty * 100, 4) pct, value
  from provider_ok cross join all_enc
  group by value, all_enc.qty
) t
;


insert into test_cases (query_name, description, pass, freq, pct, value)
select 'enc_have_discharge_date' query_name
     , '
"Discharge date. Should be populated for all
Inpatient Hospital Stay (IP) and Non-Acute
Institutional Stay (IS) encounter types."
 -- PCORnet CDM v3
' description
     , case when t.freq = 0 then 1 else 0 end pass
     , t.*
from (
  with enc_agg as (
    select count(*) enc_qty from encounter
    where enc_type in ('IP', 'IS')
    )
  select count(*) freq
       , round(count(*) / enc_qty * 100, 1) pct
       , enc_type value
  from (
  select *
  from encounter
  where enc_type in ('IP', 'IS')
  and discharge_date is null
  ) problems
  cross join enc_agg
  group by enc_qty, enc_type
  ) t
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

select case when count(*) > 0 then 1/0 else 1 end all_test_cases_pass from (
select * from test_cases where pass = 0
);

