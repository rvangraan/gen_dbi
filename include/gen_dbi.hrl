%%--------------------------------------------------------------------------------------------------
-record(gen_dbi_dbh,{
  driver = gen_dbd_pg,
  handle = undefined
}).
%%--------------------------------------------------------------------------------------------------
-record(gen_dbi_sth, {
  driver = gen_dbd_pg,
  handle = undefined,
  statement = undefined
}).
%%--------------------------------------------------------------------------------------------------