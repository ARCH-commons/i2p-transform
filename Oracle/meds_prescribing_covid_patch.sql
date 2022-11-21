MERGE
INTO    pcornet_cdm.PRESCRIBING trg
USING   (
		SELECT 
		 p.rowid    AS rid
		,TCCMM.rxcui AS new_RXNORM_CUI
		FROM PCORNET_CDM.PRESCRIBING p
		JOIN lpatel.TEMP_CDC_COVID_MED_MAPPING tccmm ON 'KUH|MEDICATION_ID:' || tccmm.medication_id = RAW_RXNORM_CUI
		WHERE p.RXNORM_CUI IS NULL
        ) src
ON      (trg.rowid = src.rid)
WHEN MATCHED THEN UPDATE
    SET trg.RXNORM_CUI = src.new_RXNORM_CUI
 ;
--Updated Rows	7031

MERGE
INTO    pcornet_cdm.MED_ADMIN trg
USING   (
		SELECT
     	 ma.rowid    AS rid
     	,'RX' AS NEW_MEDADMIN_TYPE
		,TCCMM.rxcui AS NEW_RXNORM_CUI
		FROM PCORNET_CDM.MED_ADMIN ma 
		JOIN lpatel.TEMP_CDC_COVID_MED_MAPPING tccmm ON 'KUH|MEDICATION_ID:' || tccmm.medication_id = ma.RAW_MEDADMIN_CODE
		WHERE (ma.MEDADMIN_TYPE IS NULL OR ma.MEDADMIN_TYPE = 'RX')
			AND ma.MEDADMIN_CODE IS NULL
        ) src
ON      (trg.rowid = src.rid)
WHEN MATCHED THEN UPDATE
    SET trg.MEDADMIN_CODE = src.NEW_RXNORM_CUI,
        trg.MEDADMIN_TYPE = src.NEW_MEDADMIN_TYPE
;
----Updated Rows	3149
COMMIT;
