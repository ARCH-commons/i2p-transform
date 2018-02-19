--------------------------------------------------------------------------------
-- OBS_GEN
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE obs_gen');
END;
/
CREATE TABLE obs_gen(
    OBSGENID varchar(50) NOT NULL,
    PATID varchar(50) NOT NULL,
    ENCOUNTERID varchar(50) NULL,
    OBSGEN_PROVIDERID varchar(50) NULL,
    OBSGEN_DATE date NULL,
    OBSGEN_TIME varchar(5) NULL,
    OBSGEN_TYPE varchar(30) NULL,
    OBSGEN_CODE varchar(50) NULL,
    OBSGEN_RESULT_QUAL varchar(50) NULL,
    OBSGEN_RESULT_TEXT varchar(50) NULL,
    OBSGEN_RESULT_NUM NUMBER(18, 0) NULL, -- (8,0)
    OBSGEN_RESULT_MODIFIER varchar(2) NULL,
    OBSGEN_RESULT_UNIT varchar(50) NULL,
    OBSGEN_TABLE_MODIFIED varchar(3) NULL,
    OBSGEN_ID_MODIFIED varchar(50) NULL,
    RAW_OBSGEN_NAME varchar(50) NULL,
    RAW_OBSGEN_CODE varchar(50) NULL,
    RAW_OBSGEN_TYPE varchar(50) NULL,
    RAW_OBSGEN_RESULT varchar(50) NULL,
    RAW_OBSGEN_UNIT varchar(50) NULL
)
/