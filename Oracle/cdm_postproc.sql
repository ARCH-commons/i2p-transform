/* Post-processing tasks against the CDM
*/

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

/* Currently in HERON, we have hight in cm and weight in oz (from visit vitals).
The CDM wants height in inches and weight in pounds. */
update vital v set v.ht = v.ht / 2.54;
update vital v set v.wt = v.wt / 16;

/*
 * FIX: In order to populate RX_DAYS_SUPPLY for Cycle 2 we calculate it from 
 * RX_QUANTITY and RX_FREQUENCY.  For Cycle 3 this should be removed as days 
 * supply values should make their way in from HERON.
 */
whenever sqlerror continue;
drop table pcori_freq_mapping;
whenever sqlerror exit;

create table pcori_freq_mapping as (
  select '01' rx_frequency, 1 freq_daily from dual union all -- Every day
  select '02' rx_frequency, 2 freq_daily from dual union all -- Two times a day
  select '03' rx_frequency, 3 freq_daily from dual union all -- Three times a day
  select '04' rx_frequency, 4 freq_daily from dual union all -- Four times a day
  select '05' rx_frequency, 1 freq_daily from dual union all -- Every morning
  select '06' rx_frequency, 1 freq_daily from dual union all -- Every afternoon
  select '07' rx_frequency, 3 freq_daily from dual union all -- Before meals
  select '08' rx_frequency, 3 freq_daily from dual -- After meals
);

update prescribing pres 
set pres.rx_days_supply = (
  select ceil(pres.rx_quantity / freq.freq_daily)
  from pcori_freq_mapping freq
where pres.rx_frequency=freq.rx_frequency
  and pres.rx_quantity is not null
);