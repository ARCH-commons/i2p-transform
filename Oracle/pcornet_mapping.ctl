options (errors=0, skip=1)
load data
truncate into table pcornet_mapping
fields terminated by ',' optionally enclosed by '"'
trailing nullcols(
  PCORI_PATH,
  LOCAL_PATH
  )