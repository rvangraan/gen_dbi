%%--------------------------------------------------------------------------------------------------
-record(gen_dbi_dbh,{
  driver        = gen_dbd_pg,
  driver_config = undefined,
  handle        = undefined
}).
%%--------------------------------------------------------------------------------------------------
-record(gen_dbi_sth, {
  driver        = gen_dbd_pg,
  driver_config = undefined,  
  handle        = undefined,
  statement     = undefined
}).
%%--------------------------------------------------------------------------------------------------