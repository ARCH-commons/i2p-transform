/** patient_chunk_survery - create table to save ntile over patient_num
*/

whenever sqlerror continue;
  drop table patient_chunks;
whenever sqlerror exit;
/
create table patient_chunks (
        chunk_qty integer not null,
        chunk_num integer not null,
        patient_num_qty integer not null,
        patient_num_first varchar2(64),
        patient_num_last varchar2(64),
        constraint patient_chunks_pk primary key (chunk_qty, chunk_num),
        constraint chunk_qty_pos check (chunk_qty > 0),
        constraint chunk_num_in_range check (chunk_num between 1 and chunk_qty),
        constraint chunk_first check (patient_num_first is not null or chunk_num = 1)
        );

-- Can we refer to the table without error?
--select coalesce((select 1 from patient_chunks where patient_id_qty > 0 and rownum=1), 1) complete
--from dual;
/
insert into patient_chunks (
          chunk_qty
        , chunk_num
        , patient_num_qty
        , patient_num_first
        , patient_num_last
)
select :chunk_qty
    , chunk_num
    , count(distinct patient_num) patient_num_qty
    , min(patient_num) patient_num_first
    , max(patient_num) patient_num_last
    from (
        select patient_num, ntile(:chunk_qty) over (order by patient_num) as chunk_num
        from (select distinct patient_num from BLUEHERONDATA.patient_dimension))
group by chunk_num, :chunk_qty
order by chunk_num;
/
select case
    when (select count(distinct chunk_num)
    from patient_chunks
    where chunk_qty = &&chunk_qty) = &&chunk_qty
    then 1
    else 0
    end complete
from dual;
