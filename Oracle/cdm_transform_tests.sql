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
