--------------------------------------------------------------------------------
-- DEATH_CAUSE
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE death_cause');
END;
/
CREATE TABLE death_cause(
	PATID varchar(50) NOT NULL,
	DEATH_CAUSE varchar(8) NOT NULL,
	DEATH_CAUSE_CODE varchar(2) NOT NULL,
	DEATH_CAUSE_TYPE varchar(2) NOT NULL,
	DEATH_CAUSE_SOURCE varchar(2) NOT NULL,
	DEATH_CAUSE_CONFIDENCE varchar(2) NULL
)
/
--SELECT count(PATID) from death_cause where rownum = 1
SELECT count(MEDADMINID) from med_admin where rownum = 1