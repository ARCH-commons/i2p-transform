/** provider - create and populate the provider table.
*/
BEGIN
PMN_DROPSQL('DROP TABLE provider');
END;
/
CREATE TABLE provider(
    PROVIDERID varchar(50) NOT NULL,
    PROVIDER_SEX varchar(2) NULL,
    PROVIDER_SPECIALTY_PRIMARY varchar(50) NULL,
    PROVIDER_NPI NUMBER(18, 0) NULL, -- (8,0)
    PROVIDER_NPI_FLAG varchar(1) NULL,
    RAW_PROVIDER_SPECIALTY_PRIMARY varchar(50) NULL
)
/
BEGIN
PMN_DROPSQL('DROP sequence provider_seq');
END;
/
create sequence provider_seq
/
create or replace trigger provider_trg
before insert on provider
for each row
begin
  select provider_seq.nextval into :new.PROVIDERID from dual;
end;
/
create or replace procedure PCORNetProvider as
begin

PMN_DROPSQL('drop index provider_idx');

execute immediate 'truncate table provider';

insert into provider(provider_sex, provider_specialty_primary, provider_npi, provider_npi_flag, raw_provider_specialty_primary)

;

execute immediate 'create index provider_idx on provider (PROVIDERID)';
GATHER_TABLE_STATS('PROVIDER');

end PCORNetProvider;
/
insert into cdm_status (status, last_update, records) select 'provider', sysdate, count(*) from provider
/
select 1 from cdm_status where status = 'provider'
