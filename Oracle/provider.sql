/** provider - create and populate the provider table.
*/
insert into cdm_status (task, start_time) select 'provider', sysdate from dual
/

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

create sequence provider_seq cache 2000
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

insert into provider(providerid, provider_sex, provider_specialty_primary, provider_npi, provider_npi_flag, raw_provider_specialty_primary)
select cs.prov_id
  , case when cs.sex = 'U' then 'UN'
    when cs.sex is null then 'NI'
    else cs.sex end as sex
  , case when sm.npi is null then 'NI'
    else nvl(sm.specialty, 'UN') end as provider_specialty_primary
  , cs2.npi
  , case when cs2.npi is not null then 'Y' else 'N' end as provider_npi_flag
  , sp.descriptive_text
  from clarity.clarity_ser@id cs
  left join clarity.clarity_ser_2@id cs2 on cs.prov_id = cs2.prov_id
  left join provider_specialty_map sm on sm.npi = cs2.npi
  left join provider_specialty_code sp on sm.specialty = sp.code;

execute immediate 'create index provider_idx on provider (PROVIDERID)';
GATHER_TABLE_STATS('PROVIDER');

end PCORNetProvider;
/

begin
PCORNetProvider();
end;
/

update cdm_status
set end_time = sysdate, records = (select count(*) from provider)
where task = 'provider'
/

select 1 from cdm_status where task = 'provider'
