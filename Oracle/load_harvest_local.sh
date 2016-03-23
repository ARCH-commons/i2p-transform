#!/bin/bash
set -e

# Expected environment variables (put there by Jenkins, etc.)

# Database SID
# export sid=

# User and password for CDM user
# export pcornet_cdm_user=
# export pcornet_cdm=

# Create/Load the local harvest data
sqlplus /nolog <<EOF
connect ${pcornet_cdm_user}/${pcornet_cdm}

set echo on;

WHENEVER SQLERROR CONTINUE;
drop table harvest_local;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

start harvest_local_ddl.sql
EOF

ORACLE_SID=${sid} sqlldr ${pcornet_cdm_user}/${pcornet_cdm} control=harvest_local.ctl data=harvest_local.csv bad=harvest_local.bad log=harvest_local.log errors=0
