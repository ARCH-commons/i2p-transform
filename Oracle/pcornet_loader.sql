/** pcornet_loader - perform post-processing operations.
*/
insert into cdm_status (task, start_time) select 'pcornet_loader', sysdate from dual
/
create or replace procedure PCORNetPostProc as
begin

  /* Copy providerid from encounter table to diagnosis, procedures tables.
  CDM specification says:
    "Please note: This is a field replicated from the ENCOUNTER table."
  */
  merge into diagnosis d
  using encounter e
     on (d.encounterid = e.encounterid)
  when matched then update set d.providerid = e.providerid;

  merge into procedures p
  using encounter e
     on (p.encounterid = e.encounterid)
  when matched then update set p.providerid = e.providerid;

  merge into prescribing p
  using encounter e
     on (p.encounterid = e.encounterid)
  when matched then update set p.rx_providerid = e.providerid;

  update pcornet_cdm.prescribing
  set rx_providerid = null where
  rx_providerid = '@';
  
  merge into prescribing p
  using pcornet_cdm.rxnorm_mapping e
     on (p.raw_rxnorm_cui = e.raw_rxnorm_cui)
  when matched then update set p.rxnorm_cui = e.rxnorm_cui;

  /* Currently in HERON, we have height in cm and weight in oz (from visit vitals).
  The CDM wants height in inches and weight in pounds. */
  update vital v set v.ht = v.ht / 2.54;
  update vital v set v.wt = v.wt / 16;
  
  /* Result units used by KUH are mg/dL but the CDM spec requires a gm/dL*/
  update pcornet_cdm.lab_result_cm
  set result_num=result_num/1000
  where lab_loinc in('2862-1','26474-7');
  
  update pcornet_cdm.lab_result_cm
  set lab_loinc='48642-3'
  where raw_facility_code like '%KUH|COMPONENT_ID:191';

  update pcornet_cdm.lab_result_cm
  set lab_loinc='48643-1'
  where raw_facility_code like '%KUH|COMPONENT_ID:200';
  
  update pcornet_cdm.obs_clin lab
  set lab.obsclin_result_unit = (SELECT mc.ucum_code FROM pcornet_cdm.resultunit_manualcuration mc WHERE lab.obsclin_result_unit = mc.result_unit);
  
  update pcornet_cdm.obs_clin lab
  set lab.obsclin_code=null
  where obsclin_type='NI';
  
  update pcornet_cdm.obs_gen
  set obsgen_type='LC'
  where obsgen_code is not null;

  /* Remove rows from the PRESCRIBING table where RX_* fields are null
     TODO: Remove this when fixed in HERON
   */
  delete
  from prescribing
  where rx_basis is null
    and rx_quantity is null
    and rx_frequency is null
    and rx_refills is null
  ;
  
   /* Remove ICD-9 rows from the procedures table that don't follow CDM specifications
   ICD-9-CM (09): 3-4 numbers, including a decimal
   */
    delete 
    from procedures
    where PX_type = '09'
    and  not regexp_like(px, '[.?0-9-]{3,4}'); 

    /* Remove ICD-10 rows from the procedures table that don't follow CDM specifications
   ICD-10-CM (09): 7 alphanumeric characters
   */
    delete 
    from procedures
    where PX_type = '10'
    and not regexp_like(px, '[A-Za-z0-9-]{7}') ; 

  /* Removed bad NDC code which make their way in from the source system
     (i.e 00000000000 and 99999999999) */
  delete from dispensing
  where ndc in ('00000000000', '99999999999') or length(ndc)<11 or ndc like '00NDL%' or ndc like '00SYR%'
  ;
 
 update pcornet_cdm.diagnosis
 set pdx='NI'
 where pdx='1';
 
 update pcornet_cdm.lds_address_history
 set address_zip5= null
 where not regexp_like (address_zip5, '\d{5}');
 
 update pcornet_cdm.lds_address_history
 set address_zip9= null
 where not regexp_like (address_zip9, '\d{9}');
 
 delete from pcornet_cdm.death
 where patid not in(select patid from pcornet_cdm.demographic);

end PCORNetPostProc;
/
BEGIN
PCORNetPostProc();
END;
/
update cdm_status
set end_time = sysdate, records = 0
where task = 'pcornet_loader'
/
select records + 1 from cdm_status where task = 'pcornet_loader'
