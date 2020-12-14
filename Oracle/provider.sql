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

create or replace procedure PCORNetProvider as
begin
PMN_DROPSQL('drop index provider_idx');

execute immediate 'truncate table provider';

insert into provider(providerid, provider_sex, provider_specialty_primary, provider_npi, provider_npi_flag, raw_provider_specialty_primary)
select pd.provider_id
  , case when pd.provider_sex = 'U' then 'UN'
    when pd.provider_sex is null then 'NI'
    else pd.provider_sex end as sex
  , case when sm.npi is null then 'NI'
    else nvl(sm.specialty, 'UN') end as provider_specialty_primary
  , pd.provider_npi
  , case when pd.provider_npi is not null then 'Y' else 'N' end as provider_npi_flag
  , substr(sp.descriptive_text, 1, 50)
  from "&&i2b2_data_schema".provider_dimension pd
  left join provider_specialty_map sm on sm.npi = pd.provider_npi
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

select records from cdm_status where task = 'provider'
