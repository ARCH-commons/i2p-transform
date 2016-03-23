options (errors=0, skip=1)
load data
truncate into table harvest_local
fields terminated by ',' optionally enclosed by '"'
trailing nullcols(
    DATAMART_CLAIMS,
    DATAMART_EHR,
    BIRTH_DATE_MGMT,
    ENR_START_DATE_MGMT,
    ENR_END_DATE_MGMT,
    ADMIT_DATE_MGMT,
    DISCHARGE_DATE_MGMT,
    PX_DATE_MGMT,
    RX_ORDER_DATE_MGMT,
    RX_START_DATE_MGMT,
    RX_END_DATE_MGMT,
    DISPENSE_DATE_MGMT,
    LAB_ORDER_DATE_MGMT,
    SPECIMEN_DATE_MGMT,
    RESULT_DATE_MGMT,
    MEASURE_DATE_MGMT,
    ONSET_DATE_MGMT,
    REPORT_DATE_MGMT,
    RESOLVE_DATE_MGMT,
    PRO_DATE_MGMT
    )


