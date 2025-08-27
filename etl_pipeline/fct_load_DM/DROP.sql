DROP SCHEMA IF EXISTS bl_dm CASCADE;

-- Drop loaders / utilities in bl_cl schema if they exist
DROP PROCEDURE IF EXISTS bl_cl.dm_load_product()      CASCADE;
DROP PROCEDURE IF EXISTS bl_cl.dm_load_retailer()     CASCADE;
DROP PROCEDURE IF EXISTS bl_cl.dm_load_geography()    CASCADE;
DROP PROCEDURE IF EXISTS bl_cl.dm_load_methods()      CASCADE;
DROP PROCEDURE IF EXISTS bl_cl.dm_load_customer()     CASCADE;
DROP PROCEDURE IF EXISTS bl_cl.dm_load_all()          CASCADE;
DROP PROCEDURE IF EXISTS bl_cl.dm_build_date(date,date) CASCADE;

DROP FUNCTION  IF EXISTS bl_cl.fn_date_key(date)      CASCADE;

-- Drop composite type if it exists
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
    WHERE t.typname='t_src_pair' AND n.nspname='bl_cl'
  ) THEN
    EXECUTE 'DROP TYPE bl_cl.t_src_pair CASCADE';
  END IF;
END $$;



DROP SCHEMA IF EXISTS ctl CASCADE;

-- Recreate it clean
CREATE SCHEMA if not exists ctl;

-- Recreate the log table
CREATE TABLE ctl.etl_log (
  log_id         BIGSERIAL PRIMARY KEY,
  log_ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
  procedure_name TEXT NOT NULL,
  rows_affected  BIGINT NOT NULL,
  message        TEXT
);

-- Recreate the logging procedure
CREATE OR REPLACE PROCEDURE ctl.sp_log(
    p_proc TEXT, p_rows BIGINT, p_msg TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO ctl.etl_log(procedure_name, rows_affected, message)
  VALUES (p_proc, COALESCE(p_rows,0), p_msg);
END;
$$;