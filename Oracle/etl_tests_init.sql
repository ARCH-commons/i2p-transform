/* etl_tests_init -- Initialize ETL tests

Copyright (c) 2015 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

References:
  * KUMC Informatics ticket #2781
*/

whenever sqlerror continue;
drop table etl_tests;
whenever sqlerror exit;

create table etl_tests (
  /* Description of test */
  test_domain varchar(256) not null -- Such as top-level folder in HERON that's affected (Frontiers Registry...)
  , test_name varchar(1024) not null -- User friendly name (hictr_mrn_leading_zero...)
  /* Pass/Fail criteria - can define one or both of the following - INCLUSIVE */
  , lower_bound number null
  , upper_bound number null
  , units varchar(256) null -- What are we counting?  (patients, facts, concepts, etc.)
  , test_description varchar(4000) null -- More detailed description - how is the end-user affected if this test fails?
  );

alter table etl_tests add constraint pk_etl_tests primary key (test_domain, test_name);

-- Table to store test values (inserted during ETL) 
whenever sqlerror continue;
drop table etl_test_values;
whenever sqlerror exit;

create table etl_test_values as
  select test_domain, test_name from etl_tests where 1=0;

alter table etl_test_values
  add (test_value number not null
      , detail_char_1 varchar(256)
      , detail_char_2 varchar(256)
      , detail_num_1 number
      , detail_num_2 number
      , test_note varchar(256)
      , result_id number not null
      , result_date date not null);

alter table etl_test_values add constraint pk_etl_test_results primary key (result_id);

-- I wish we had "auto increment".  We can use a trigger but then we need PL/SQL.
whenever sqlerror continue;
drop sequence sq_result_id;
whenever sqlerror exit;
create sequence sq_result_id;

/* Insert some example tests for debugging. */
-- Example test that doesn't have something in the CSV
insert into etl_test_values (test_value, test_domain, test_name, result_id, result_date)
with test_key as (
  select 'test' test_domain,
  'test_not_found_in_csv' test_name from dual
  )
select 666 test_value, test_key.*, sq_result_id.nextval, sysdate from test_key;

-- Example successful test
insert into etl_test_values (test_value, test_domain, test_name, result_id, result_date)
with test_key as (
  select 'test' test_domain,
  'test_success' test_name from dual
  )
select 1 test_value, test_key.*, sq_result_id.nextval, sysdate from test_key;

-- Example test with no bounds specified
insert into etl_test_values (test_value, test_domain, test_name, result_id, result_date)
with test_key as (
  select 'test' test_domain,
  'test_no_bound' test_name from dual
  )
select 1 test_value, test_key.*, sq_result_id.nextval, sysdate from test_key;


/* complete check */
select 1 complete
from (select test_name from etl_test_values where 1=0
      union all select * from dual -- guarantee 1 row
      );
