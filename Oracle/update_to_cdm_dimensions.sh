#!/bin/bash
set -e

# Expected environment variables (put there by Jenkins, etc.)
# export pcornet_cdm_user=
# export pcornet_cdm=

sqlplus /nolog <<EOF
connect ${pcornet_cdm_user}/${pcornet_cdm}

set echo on;
define i2b2_data_schema=${i2b2_data_schema}

WHENEVER SQLERROR EXIT SQL.SQLCODE;

start update_ethnicity_pdim.sql
start heron_encounter_style.sql
EOF
