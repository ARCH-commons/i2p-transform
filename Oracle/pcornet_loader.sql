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

  /* Currently in HERON, we have height in cm and weight in oz (from visit vitals).
  The CDM wants height in inches and weight in pounds. */
  update vital v set v.ht = v.ht / 2.54;
  update vital v set v.wt = v.wt / 16;

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

  /* Removed bad NDC code which make their way in from the source system
     (i.e 00000000000 and 99999999999) */
  delete from dispensing
  where ndc in ('00000000000', '99999999999')
  ;

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
select 1 from cdm_status where status = 'pcornet_loader'