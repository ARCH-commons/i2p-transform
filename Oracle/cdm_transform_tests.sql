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
/* Test to make sure we have something about patient smoking tobacco use */
with smokers as (
  select count(*) qty from vital where smoking!='NI'
)
select case when smokers.qty > 0 then 1 else 1/0 end smoker_count_ok from smokers;

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
