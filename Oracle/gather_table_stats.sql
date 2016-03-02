/* Metadata table statistics - experimentation shows vastly better performance
if this is done before the CDM transform.

ACK: http://stackoverflow.com/questions/2242024/for-each-string-execute-a-function-procedure
*/
declare
table_list sys.dbms_debug_vc2coll := sys.dbms_debug_vc2coll(
  'PCORNET_DEMO', 'PCORNET_DIAG', 'PCORNET_ENC', 'PCORNET_ENROLL', 'PCORNET_LAB', 
  'PCORNET_MED', 'PCORNET_PROC', 'PCORNET_VITAL');
begin 
  for t in table_list.first .. table_list.last
  loop
    DBMS_STATS.GATHER_TABLE_STATS (
            ownname => '"&&i2b2_meta_schema"',
            tabname => table_list(t),
            estimate_percent => 40
            );
  end loop;
end;
/
