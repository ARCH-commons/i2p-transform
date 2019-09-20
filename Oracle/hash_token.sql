/** harvest - create and populate the hash_token table.
*/
insert into cdm_status (task, start_time) select 'hash_token', sysdate from dual
/
BEGIN
PMN_DROPSQL('DROP TABLE hash_token');
END;
/

create table HASH_TOKEN (
PATID varchar(50) NOT NULL,
TOKEN_01 varchar(50),
TOKEN_02 varchar(50),
TOKEN_05 varchar(50),
TOKEN_12 varchar(50),
TOKEN_17 varchar(50),
TOKEN_21 varchar(50),
TOKEN_22 varchar(50),
TOKEN_23 varchar(50),
CONSTRAINT pk_hash_token_pat PRIMARY KEY (patid),
CONSTRAINT fk_hash_dem_patid
    FOREIGN KEY (patid)
    REFERENCES demographic (patid)
);

/
update cdm_status
set end_time = sysdate, records = (select count(*) from hash_token)
where task = 'hash_token'
/