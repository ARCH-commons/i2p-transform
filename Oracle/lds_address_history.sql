/** immunization - create and populate the immunization table.
*/
insert into cdm_status (task, start_time) select 'lds_address_history', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE lds_address_history');
END;
/
CREATE TABLE lds_address_history ( 
    ADDRESSID VARCHAR(20 BYTE) NOT NULL, 
	PATID VARCHAR(50 BYTE) NOT NULL, 
	ADDRESS_USE VARCHAR(2 BYTE) NOT NULL, 
	ADDRESS_TYPE VARCHAR(2 BYTE) NOT NULL, 
	ADDRESS_PREFERRED VARCHAR(2 BYTE) NOT NULL, 
	ADDRESS_CITY VARCHAR(50 BYTE), -- there some null city
	ADDRESS_STATE VARCHAR(2 BYTE), -- there some null state
	ADDRESS_ZIP5 VARCHAR(5 BYTE),  -- there some null zip5
	ADDRESS_ZIP9 VARCHAR(9 BYTE),  -- there some null zip9
	ADDRESS_PERIOD_START DATE NOT NULL,
    ADDRESS_PERIOD_END DATE
   )
/
BEGIN
PMN_DROPSQL('DROP sequence  lds_address_history_seq');
END;
/
create sequence  lds_address_history_seq
/
create or replace trigger lds_address_history_trg
before insert on lds_address_history
for each row
begin
  select lds_address_history_seq.nextval into :new.ADDRESSID from dual;
end;
/
insert /*+ APPEND */ into lds_address_history(
     PATID
	,ADDRESS_USE
	,ADDRESS_TYPE
	,ADDRESS_PREFERRED
	,ADDRESS_CITY
	,ADDRESS_STATE
	,ADDRESS_ZIP5
	,ADDRESS_ZIP9
	,ADDRESS_PERIOD_START
    ,ADDRESS_PERIOD_END
)
select 
     patient_num patid
    , 'HO' ADDRESS_USE --hardcoding and assuming all addresses will be home address 
    , case 
        when 
            upper(add_line_1) like '%BOX%' 
            and
                (
                upper(add_line_1) like '%PO%' 
                or upper(add_line_1) like '%P O%'
                or upper(add_line_1) like '%P.O.%'
                )
        then 'PO'
        ELSE 'PH'
    END  ADDRESS_TYPE
    , 'Y' ADDRESS_PREFERRED --we are only storing one address per patients, so assuming that is prefered address
    , city ADDRESS_CITY
    , scode.code ADDRESS_STATE
    , CASE 
            WHEN length(zip_cd)=5 
                THEN zip_cd
            WHEN length(zip_cd)>5 
                THEN substr(zip_cd,1,5)
            else null
      END ADDRESS_ZIP5
    , CASE 
            WHEN length(zip_cd)=10 or length(zip_cd)=10 -- some time there is dash in zip, so length can be 9 or 10
                THEN replace(zip_cd,'-','')
            else null
      END ADDRESS_ZIP9
    , download_date ADDRESS_PERIOD_START 
    , null ADDRESS_PERIOD_END
        
from "&&i2b2_data_schema".patient_dimension pdim
left join pcornet_cdm.state_code scode 
    on upper(scode.state) = upper(pdim.state_cd)
/
commit
/
update cdm_status
set end_time = sysdate, records = (select count(*) from lds_address_history)
where task = 'lds_address_history'
/

select records from cdm_status where task = 'lds_address_history'
