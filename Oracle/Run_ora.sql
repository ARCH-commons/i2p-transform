----------------------------------------------------------------------------------------------------------------------------------------
-- Run i2p Transform in Oracle
-- Written by Matthew Joss
----------------------------------------------------------------------------------------------------------------------------------------

BEGIN          -- RUN PROGRAM 
pcornetclear;  -- Make sure to run this before re-populating any pmn tables.
pcornetloader; -- you may want to run sql statements one by one in the pcornetloader procedure :)
END;
/
