BEGIN;
DROP TABLE IF EXISTS
  bl_3nf.ce_sales,
  bl_3nf.ce_customer_scd,
  bl_3nf.ce_product,
  bl_3nf.ce_category,
  bl_3nf.ce_retailer_lineage,
  bl_3nf.ce_sales_method,
  bl_3nf.ce_payment_method,
  bl_3nf.ce_city,
  bl_3nf.ce_state,
  bl_3nf.ce_region
CASCADE;

TRUNCATE ctl.etl_log;

COMMIT;




-- Make names resolve
SET search_path = public, ctl, bl_cl, bl_3nf, sa_instore, sa_online;

BEGIN;

-- A) Drop ETL functions/procedures (no data touched yet)
DROP FUNCTION  IF EXISTS bl_cl.fn_load_ce_retailer();
DROP PROCEDURE IF EXISTS bl_cl.sp_load_ce_category();
DROP PROCEDURE IF EXISTS bl_cl.sp_load_ce_product();
DROP PROCEDURE IF EXISTS bl_cl.sp_load_ce_retailer();
DROP PROCEDURE IF EXISTS bl_cl.sp_load_ce_methods();
DROP PROCEDURE IF EXISTS bl_cl.sp_load_geo();
DROP PROCEDURE IF EXISTS bl_cl.sp_load_ce_customer_scd();
DROP PROCEDURE IF EXISTS bl_cl.sp_load_ce_sales();

-- B) Clear 3NF data but keep default rows (id = -1)
--    Order matters because of FKs.

-- Fact first
TRUNCATE bl_3nf.ce_sales RESTART IDENTITY;

-- Child → parent order for GEO
DELETE FROM bl_3nf.ce_city   WHERE city_id   <> -1;
ALTER TABLE bl_3nf.ce_city   ALTER COLUMN city_id   RESTART WITH 1;

DELETE FROM bl_3nf.ce_state  WHERE state_id  <> -1;
ALTER TABLE bl_3nf.ce_state  ALTER COLUMN state_id  RESTART WITH 1;

DELETE FROM bl_3nf.ce_region WHERE region_id <> -1;
ALTER TABLE bl_3nf.ce_region ALTER COLUMN region_id RESTART WITH 1;

-- Dimensions (respect FKs: product → category)
DELETE FROM bl_3nf.ce_product  WHERE product_id  <> -1;
ALTER TABLE bl_3nf.ce_product  ALTER COLUMN product_id RESTART WITH 1;

DELETE FROM bl_3nf.ce_category WHERE category_id <> -1;
ALTER TABLE bl_3nf.ce_category ALTER COLUMN category_id RESTART WITH 1;

DELETE FROM bl_3nf.ce_retailer WHERE retailer_id <> -1;
ALTER TABLE bl_3nf.ce_retailer ALTER COLUMN retailer_id RESTART WITH 1;

DELETE FROM bl_3nf.ce_sales_method WHERE sales_method_id <> -1;
ALTER TABLE bl_3nf.ce_sales_method ALTER COLUMN sales_method_id RESTART WITH 1;

DELETE FROM bl_3nf.ce_payment_method WHERE payment_method_id <> -1;
ALTER TABLE bl_3nf.ce_payment_method ALTER COLUMN payment_method_id RESTART WITH 1;

-- SCD2 (keep default row id = -1)
DELETE FROM bl_3nf.ce_customer_scd WHERE customer_id <> -1;
ALTER TABLE bl_3nf.ce_customer_scd ALTER COLUMN customer_id RESTART WITH 1;

-- C) Clear ETL log
TRUNCATE ctl.etl_log;

COMMIT;
