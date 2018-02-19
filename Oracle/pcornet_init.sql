--------------------------------------------------------------------------------
-- HELPER FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------

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


--------------------------------------------------------------------------------
-- I2B2 SYNONYMS, VIEWS, AND INTERMEDIARY TABLES
--------------------------------------------------------------------------------


CREATE OR REPLACE SYNONYM I2B2FACT FOR "&&i2b2_data_schema".OBSERVATION_FACT
/

CREATE OR REPLACE SYNONYM I2B2MEDFACT FOR OBSERVATION_FACT_MEDS
/

BEGIN
PMN_DROPSQL('DROP TABLE i2b2patient_list');
END;
/

CREATE table i2b2patient_list as
select * from
(
select DISTINCT PATIENT_NUM from I2B2FACT where START_DATE > to_date('&&min_pat_list_date_dd_mon_rrrr','dd-mon-rrrr')
) where ROWNUM<100000000
/

create or replace VIEW i2b2patient as select * from "&&i2b2_data_schema".PATIENT_DIMENSION where PATIENT_NUM in (select PATIENT_NUM from i2b2patient_list)
/

create or replace view i2b2visit as select * from "&&i2b2_data_schema".VISIT_DIMENSION where START_DATE >= to_date('&&min_visit_date_dd_mon_rrrr','dd-mon-rrrr') and (END_DATE is NULL or END_DATE < CURRENT_DATE) and (START_DATE <CURRENT_DATE)
/

CREATE OR REPLACE SYNONYM pcornet_med FOR  "&&i2b2_meta_schema".pcornet_med
/

CREATE OR REPLACE SYNONYM pcornet_lab FOR  "&&i2b2_meta_schema".pcornet_lab
/

CREATE OR REPLACE SYNONYM pcornet_diag FOR  "&&i2b2_meta_schema".pcornet_diag
/

CREATE OR REPLACE SYNONYM pcornet_demo FOR  "&&i2b2_meta_schema".pcornet_demo
/

CREATE OR REPLACE SYNONYM pcornet_proc FOR  "&&i2b2_meta_schema".pcornet_proc
/

CREATE OR REPLACE SYNONYM pcornet_vital FOR  "&&i2b2_meta_schema".pcornet_vital
/

CREATE OR REPLACE SYNONYM pcornet_enc FOR  "&&i2b2_meta_schema".pcornet_enc
/
select 1 from dual