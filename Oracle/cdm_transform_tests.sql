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
select case when sum(calc.tst) < 3 then 1/0 else 1 end pass from calc;


/* Test to make sure we have something about patient general tobacco use */
with tobacco as (
  select count(*) qty from vital where tobacco!='NI'
)
select case when tobacco.qty > 0 then 1 else 1/0 end tobacco_count_ok from tobacco;





