--------------------------------------------------------------------------------
-- PCORNetLogging Script
--
-- This file contains the Oracle SQL needed to create the logging table and 
-- associated sequence.
--------------------------------------------------------------------------------


whenever sqlerror continue;
drop table i2p_task_log;
whenever sqlerror exit;

create table i2p_task_log (
  task_num number(8) not null,
  cdm_release varchar(48) not null,
  task_name varchar(48) not null,
  build_num number(8) not null,
  started date not null,
  completed date,
  num_rows number(16),
  data_source varchar(48)
);


whenever sqlerror continue;
drop sequence i2p_task_seq;
whenever sqlerror exit;

create sequence i2p_task_seq
  minvalue 1
  start with 1
  increment by 1
  nocache
;


create or replace procedure LogTaskStart(r_name varchar2, t_name varchar2, b_num number, d_source varchar2) as
begin
  insert into i2p_task_log (task_num, release_name, task_name, build_num, started, data_source)
  values (i2p_task_seq.nextval, r_name, t_name, b_num, sysdate, d_source);
end LogTaskStart;


create or replace procedure LogTaskComplete(r_name varchar2, t_name varchar2, b_num number, target_table varchar2) as
begin
  execute immediate 
  'update i2p_task_log set (completed, num_rows) = (' ||
  '  select sysdate, count(*) from ' || target_table ||
  ' )' ||
  'where release_name=''' || r_name || '''' ||
  '  and task_name=''' || t_name || '''' ||
  '  and build_num=''' || b_num || ''''
  ;
end LogTaskComplete;