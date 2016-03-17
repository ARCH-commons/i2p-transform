#!/bin/bash
set -e

# Expected environment variables
# Database SID
# export sid=

# User and password for CDM user
# export pcornet_cdm_user=
# export pcornet_cdm=

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
