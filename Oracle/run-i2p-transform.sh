# Expected environment variables (put there by Jenkins, etc.)

# Database SID
#export sid=
# User and password for CDM user
#export pcornet_cdm_user=
#export pcornet_cdm=

#export i2b2_data_schema=
#export i2b2_meta_schema=
#export datamart_id=
#export datamart_name=
#export network_id=
#export network_name=

# All i2b2 terms - used for local path mapping
#export terms_table=

# Create/Load the local path mapping table
sqlplus /nolog <<EOF
connect ${pcornet_cdm_user}/${pcornet_cdm}

set echo on;

WHENEVER SQLERROR CONTINUE;
drop table pcornet_mapping;
drop table pcornet_ontology_updates;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

start pcornet_mapping_ddl.sql
start pcornet_ontology_updates_ddl.sql
EOF

ORACLE_SID=${sid} sqlldr ${pcornet_cdm_user}/${pcornet_cdm} control=pcornet_mapping.ctl data=pcornet_mapping.csv bad=pcornet_mapping.bad log=pcornet_mapping.log errors=0

ORACLE_SID=${sid} sqlldr ${pcornet_cdm_user}/${pcornet_cdm} control=pcornet_ontology_updates.ctl data=pcornet_ontology_updates.csv bad=pcornet_ontology_updates.bad log=pcornet_ontology_updates.log errors=0

# Insert local terms as leaves of the PCORNet terms and run the transform
sqlplus /nolog <<EOF
connect ${pcornet_cdm_user}/${pcornet_cdm}

set echo on;

WHENEVER SQLERROR CONTINUE;

drop table PMN_LABNORMAL;
drop table DEMOGRAPHIC;
drop table ENROLLMENT;
drop table ENCOUNTER;
drop table DIAGNOSIS;
drop table PROCEDURES;
drop table VITAL;
drop table DISPENSING;
drop table LAB_RESULT_CM;
drop table CONDITION;
drop table PRO_CM;
drop table PRESCRIBING;
drop table PCORNET_TRIAL;
drop table DEATH;
drop table DEATH_CAUSE;
drop table HARVEST;

WHENEVER SQLERROR EXIT SQL.SQLCODE;

define i2b2_data_schema=${i2b2_data_schema}
define i2b2_meta_schema=${i2b2_meta_schema}
define datamart_id=${datamart_id}
define datamart_name=${datamart_name}
define network_id=${network_id}
define network_name=${network_name}
define terms_table=${terms_table}

-- Local terminology mapping
start pcornet_mapping.sql

-- Prepare for transform
start gather_table_stats.sql

-- SCILHS transform
start PCORNetLoader_ora.sql

-- CDM transform tests
start cdm_transform_tests.sql

quit;
EOF
