/* Metadata table statistics - experimentation shows vastly better performance
if this is done before the CDM transform.
*/
begin 
  for rec in (select table_name 
              from dba_tables 
              where owner = '&&i2b2_meta_schema' and table_name like 'PCORNET%')
  loop
    DBMS_STATS.GATHER_TABLE_STATS (
            ownname => '"&&i2b2_meta_schema"',
            tabname => rec.table_name,
            estimate_percent => 40
            );
  end loop;
end;
/
