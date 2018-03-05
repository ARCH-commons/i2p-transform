/** pcornet_init - create helper functions and procedures
*/
create or replace PROCEDURE GATHER_TABLE_STATS(table_name VARCHAR2) AS
  BEGIN
  DBMS_STATS.GATHER_TABLE_STATS (
          ownname => 'PCORNET_CDM', -- This doesn't work as a parameter for some reason.
          tabname => table_name,
          estimate_percent => 50, -- Percentage picked somewhat arbitrarily
          cascade => TRUE,
          degree => 16
          );
END GATHER_TABLE_STATS;
/


create or replace PROCEDURE PMN_DROPSQL(sqlstring VARCHAR2) AS
  BEGIN
      EXECUTE IMMEDIATE sqlstring;
  EXCEPTION
      WHEN OTHERS THEN NULL;
END PMN_DROPSQL;
/


create or replace FUNCTION PMN_IFEXISTS(objnamestr VARCHAR2, objtypestr VARCHAR2) RETURN BOOLEAN AS
cnt NUMBER;
BEGIN
  SELECT COUNT(*)
   INTO cnt
    FROM USER_OBJECTS
  WHERE  upper(OBJECT_NAME) = upper(objnamestr)
         and upper(object_type) = upper(objtypestr);

  IF( cnt = 0 )
  THEN
    --dbms_output.put_line('NO!');
    return FALSE;
  ELSE
   --dbms_output.put_line('YES!');
   return TRUE;
  END IF;

END PMN_IFEXISTS;
/


create or replace PROCEDURE PMN_Execuatesql(sqlstring VARCHAR2) AS
BEGIN
  EXECUTE IMMEDIATE sqlstring;
  dbms_output.put_line(sqlstring);
END PMN_ExecuateSQL;
/


--ACK: http://dba.stackexchange.com/questions/9441/how-to-catch-and-handle-only-specific-oracle-exceptions
create or replace procedure create_error_table(table_name varchar2) as
sqltext varchar2(4000);

begin
  dbms_errlog.create_error_log(dml_table_name => table_name);
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE = -955 THEN
      NULL; -- suppresses ORA-00955 exception ("name is already used by an existing object")
    ELSE
       RAISE;
    END IF;
-- Delete rows from a previous run in case the table already existed
sqltext := 'delete from ERR$_' || table_name;
PMN_Execuatesql(sqltext);
end;
/


create or replace FUNCTION GETDATAMARTID RETURN VARCHAR2 IS
BEGIN
    RETURN '&&datamart_id';
END;
/


CREATE OR REPLACE FUNCTION GETDATAMARTNAME RETURN VARCHAR2 AS
BEGIN
    RETURN '&&datamart_name';
END;
/


CREATE OR REPLACE FUNCTION GETDATAMARTPLATFORM RETURN VARCHAR2 AS
BEGIN
    RETURN '02'; -- 01 is MSSQL, 02 is Oracle
END;
/


BEGIN
PMN_DROPSQL('DROP TABLE pcornet_codelist');
END;
/
create table pcornet_codelist(codetype varchar2(20), code varchar2(50))
/

create or replace procedure pcornet_parsecode (codetype in varchar, codestring in varchar) as

tex varchar(2000);
pos number(9);
readstate char(1) ;
nextchar char(1) ;
val varchar(50);

begin

val:='';
readstate:='F';
pos:=0;
tex := codestring;
FOR pos IN 1..length(tex)
LOOP
--	dbms_output.put_line(val);
    	nextchar:=substr(tex,pos,1);
	if nextchar!=',' then
		if nextchar='''' then
			if readstate='F' then
				val:='';
				readstate:='T';
			else
				insert into pcornet_codelist values (codetype,val);
				val:='';
				readstate:='F'  ;
			end if;
		else
			if readstate='T' then
				val:= val || nextchar;
			end if;
		end if;
	end if;
END LOOP;

end pcornet_parsecode;
/

create or replace procedure pcornet_popcodelist as

codedata varchar(2000);
onecode varchar(20);
codetype varchar(20);

cursor getcodesql is
select 'RACE',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\RACE%'
union
select 'SEX',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\SEX%'
union
select 'HISPANIC',c_dimcode from pcornet_demo where c_fullname like '\PCORI\DEMOGRAPHIC\HISPANIC%';

begin
open getcodesql;
LOOP
	fetch getcodesql into codetype,codedata;
	EXIT WHEN getcodesql%NOTFOUND ;
 	pcornet_parsecode (codetype,codedata );
end loop;

close getcodesql ;
end pcornet_popcodelist;
/
insert into cdm_status (status, last_update) values ('pcornet_init', sysdate)
/
select 1 from cdm_status where status = 'pcornet_init'
