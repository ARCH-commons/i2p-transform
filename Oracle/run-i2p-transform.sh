#!/bin/bash
set -e

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

# Create/Load the local path mapping and ontology update tables
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

# Run some tests
sqlplus /nolog <<EOF
connect ${pcornet_cdm_user}/${pcornet_cdm}

set echo on;

define i2b2_data_schema=${i2b2_data_schema}

WHENEVER SQLERROR EXIT SQL.SQLCODE;

-- Make sure the ethnicity code has been added to the patient dimension
-- See update_ethnicity_pdim.sql
select ethnicity_cd from "&&i2b2_data_schema".patient_dimension where 1=0;

-- Make sure that the providerid column has been added to the visit dimension
select providerid from "&&i2b2_data_schema".visit_dimension where 1=0;

-- Make sure the inout_cd has been populated
-- See heron_encounter_style.sql
select case when qty = 0 then 1/0 else 1 end inout_cd_populated from (
  select count(*) qty from "&&i2b2_data_schema".visit_dimension where inout_cd is not null
  );

EOF


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
