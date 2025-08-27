CREATE SCHEMA IF NOT EXISTS bl_cl;

-- composite type once
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
    WHERE t.typname='t_src_pair' AND n.nspname='bl_cl'
  ) THEN
    EXECUTE 'CREATE TYPE bl_cl.t_src_pair AS (src_sys text, src_ent text)';
  END IF;
END $$;

-- date_key helper (ensures dim_date row)
CREATE OR REPLACE FUNCTION bl_cl.fn_date_key(p_date date)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE v_key int;
BEGIN
  IF p_date IS NULL THEN RETURN -1; END IF;
  v_key := (EXTRACT(YEAR FROM p_date)::int*10000)
        +  (EXTRACT(MONTH FROM p_date)::int*100)
        +   EXTRACT(DAY   FROM p_date)::int;

  INSERT INTO bl_dm.dim_date(date_key, full_date, year_num, quarter_num, month_num, month_name,
                             day_of_month, day_of_week, day_name, is_weekend)
  VALUES (v_key, p_date,
          EXTRACT(YEAR FROM p_date)::int,
          EXTRACT(QUARTER FROM p_date)::int,
          EXTRACT(MONTH FROM p_date)::int,
          to_char(p_date,'Mon'),
          EXTRACT(DAY FROM p_date)::int,
          EXTRACT(ISODOW FROM p_date)::int,
          to_char(p_date,'Dy'),
          CASE WHEN EXTRACT(ISODOW FROM p_date) IN (6,7) THEN TRUE ELSE FALSE END)
  ON CONFLICT (date_key) DO NOTHING;

  RETURN v_key;
END $$;

-- optional calendar builder
CREATE OR REPLACE PROCEDURE bl_cl.dm_build_date(p_from date, p_to date)
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO bl_dm.dim_date(date_key, full_date, year_num, quarter_num, month_num, month_name,
                             day_of_month, day_of_week, day_name, is_weekend)
  SELECT
    (EXTRACT(YEAR FROM d)::int*10000 + EXTRACT(MONTH FROM d)::int*100 + EXTRACT(DAY FROM d)::int),
    d,
    EXTRACT(YEAR FROM d)::int, EXTRACT(QUARTER FROM d)::int, EXTRACT(MONTH FROM d)::int,
    to_char(d,'Mon'), EXTRACT(DAY FROM d)::int, EXTRACT(ISODOW FROM d)::int,
    to_char(d,'Dy'),
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN TRUE ELSE FALSE END
  FROM generate_series(p_from, p_to, interval '1 day') g(d)
  ON CONFLICT (date_key) DO NOTHING;

  CALL ctl.sp_log('bl_cl.dm_build_date', 0, 'dim_date built/extended');
END $$;

-- ---------- 5) DM Loader procedures ----------
-- Product (FK join only; no over-filtering)
CREATE OR REPLACE PROCEDURE bl_cl.dm_load_product()
LANGUAGE plpgsql
AS $$
DECLARE v_upd BIGINT := 0; v_ins BIGINT := 0; v_tot BIGINT := 0;
BEGIN
  WITH src AS (
    SELECT
      p.product_src_id,
      p.product_name,
      c.category_src_id,
      c.category AS category_name,
      p.source_system,
      p.source_entity
    FROM bl_3nf.ce_product p
    JOIN bl_3nf.ce_category c ON c.category_id = p.category_id
  ),
  upd AS (
    UPDATE bl_dm.dim_product d
    SET product_name    = s.product_name,
        category_src_id = s.category_src_id,
        category_name   = s.category_name
    FROM src s
    WHERE d.source_system   = s.source_system
      AND d.source_entity   = s.source_entity
      AND d.product_src_id  = s.product_src_id
      AND (
        d.product_name    IS DISTINCT FROM s.product_name OR
        d.category_src_id IS DISTINCT FROM s.category_src_id OR
        d.category_name   IS DISTINCT FROM s.category_name
      )
    RETURNING 1
  ) SELECT COUNT(*) INTO v_upd FROM upd;

  WITH src AS (
    SELECT
      p.product_src_id,
      p.product_name,
      c.category_src_id,
      c.category AS category_name,
      p.source_system,
      p.source_entity
    FROM bl_3nf.ce_product p
    JOIN bl_3nf.ce_category c ON c.category_id = p.category_id
  ),
  ins AS (
    INSERT INTO bl_dm.dim_product(product_src_id, product_name, category_src_id, category_name, source_system, source_entity)
    SELECT s.product_src_id, s.product_name, s.category_src_id, s.category_name, s.source_system, s.source_entity
    FROM src s
    WHERE NOT EXISTS (
      SELECT 1 FROM bl_dm.dim_product d
      WHERE d.source_system = s.source_system
        AND d.source_entity = s.source_entity
        AND d.product_src_id = s.product_src_id
    )
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;

  v_tot := v_upd + v_ins;
  CALL ctl.sp_log('bl_cl.dm_load_product', v_tot, 'dim_product: ins='||v_ins||', upd='||v_upd);
END $$;

-- 5.2 Retailer
CREATE OR REPLACE PROCEDURE bl_cl.dm_load_retailer()
LANGUAGE plpgsql
AS $$
DECLARE v_upd BIGINT := 0; v_ins BIGINT := 0; v_tot BIGINT := 0;
BEGIN
  WITH upd AS (
    UPDATE bl_dm.dim_retailer d
    SET retailer_name = r.retailer
    FROM bl_3nf.ce_retailer r
    WHERE d.source_system = r.source_system
      AND d.source_entity = r.source_entity
      AND d.retailer_src_id = r.retailer_src_id
      AND d.retailer_name IS DISTINCT FROM r.retailer
    RETURNING 1
  ) SELECT COUNT(*) INTO v_upd FROM upd;

  WITH ins AS (
    INSERT INTO bl_dm.dim_retailer(retailer_src_id, retailer_name, source_system, source_entity)
    SELECT r.retailer_src_id, r.retailer, r.source_system, r.source_entity
    FROM bl_3nf.ce_retailer r
    WHERE NOT EXISTS (
      SELECT 1 FROM bl_dm.dim_retailer d
      WHERE d.source_system = r.source_system
        AND d.source_entity = r.source_entity
        AND d.retailer_src_id = r.retailer_src_id
    )
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;

  v_tot := v_upd + v_ins;
  CALL ctl.sp_log('bl_cl.dm_load_retailer', v_tot, 'dim_retailer: ins='||v_ins||', upd='||v_upd);
END $$;

-- 5.3 Geography
CREATE OR REPLACE PROCEDURE bl_cl.dm_load_geography()
LANGUAGE plpgsql
AS $$
DECLARE
  v_upd BIGINT := 0;
  v_ins BIGINT := 0;
  v_tot BIGINT := 0;
BEGIN
  -- ensure ON CONFLICT target exists (safe if already there)
  DO $x$ BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conname='uq_dm_geo_city' AND conrelid='bl_dm.dim_geography'::regclass
    ) THEN
      EXECUTE 'ALTER TABLE bl_dm.dim_geography ADD CONSTRAINT uq_dm_geo_city UNIQUE (city_id)';
    END IF;
  END $x$;

  -- UPDATE changed rows (embed source as a subquery, no CTE scope issues)
  WITH upd AS (
    UPDATE bl_dm.dim_geography d
    SET city_name   = s.city_name,
        state_id    = s.state_id,
        state_name  = s.state_name,
        region_id   = s.region_id,
        region_name = s.region_name
    FROM (
      SELECT c.city_id, c.city_name,
             s.state_id, s.state_name,
             r.region_id, r.region_name
      FROM bl_3nf.ce_city  c
      JOIN bl_3nf.ce_state s  ON s.state_id  = c.state_id
      JOIN bl_3nf.ce_region r ON r.region_id = s.region_id
    ) s
    WHERE d.city_id = s.city_id
      AND (
        d.city_name  IS DISTINCT FROM s.city_name OR
        d.state_id   IS DISTINCT FROM s.state_id  OR
        d.state_name IS DISTINCT FROM s.state_name OR
        d.region_id  IS DISTINCT FROM s.region_id OR
        d.region_name IS DISTINCT FROM s.region_name
      )
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_upd FROM upd;

  -- INSERT missing rows (also using embedded subquery)
  WITH ins AS (
    INSERT INTO bl_dm.dim_geography
      (city_id, city_name, state_id, state_name, region_id, region_name)
    SELECT s.city_id, s.city_name, s.state_id, s.state_name, s.region_id, s.region_name
    FROM (
      SELECT c.city_id, c.city_name,
             s.state_id, s.state_name,
             r.region_id, r.region_name
      FROM bl_3nf.ce_city  c
      JOIN bl_3nf.ce_state s  ON s.state_id  = c.state_id
      JOIN bl_3nf.ce_region r ON r.region_id = s.region_id
    ) s
    WHERE NOT EXISTS (
      SELECT 1 FROM bl_dm.dim_geography d WHERE d.city_id = s.city_id
    )
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_ins FROM ins;

  v_tot := v_upd + v_ins;
  CALL ctl.sp_log('bl_cl.dm_load_geography', v_tot,
                  'dim_geography: ins='||v_ins||', upd='||v_upd);
END;
$$;

-- 5.4 Methods (two passes; dynamic SQL optional, here plain SQL for clarity)
CREATE OR REPLACE PROCEDURE bl_cl.dm_load_methods()
LANGUAGE plpgsql
AS $$
DECLARE v_upd BIGINT := 0; v_ins BIGINT := 0; v_tot BIGINT := 0;
BEGIN
  -- SALES METHOD
  WITH upd AS (
    UPDATE bl_dm.dim_sales_method d
    SET sales_method_name = s.sales_method
    FROM bl_3nf.ce_sales_method s
    WHERE d.source_system = s.source_system
      AND d.source_entity = s.source_entity
      AND d.sales_method_src_id = s.sales_method_src_id
      AND d.sales_method_name IS DISTINCT FROM s.sales_method
    RETURNING 1
  ) SELECT COUNT(*) INTO v_upd FROM upd;

  WITH ins AS (
    INSERT INTO bl_dm.dim_sales_method(sales_method_src_id, sales_method_name, source_system, source_entity)
    SELECT s.sales_method_src_id, s.sales_method, s.source_system, s.source_entity
    FROM bl_3nf.ce_sales_method s
    WHERE NOT EXISTS (
      SELECT 1 FROM bl_dm.dim_sales_method d
      WHERE d.source_system = s.source_system
        AND d.source_entity = s.source_entity
        AND d.sales_method_src_id = s.sales_method_src_id
    )
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;

  v_tot := v_upd + v_ins;
  CALL ctl.sp_log('bl_cl.dm_load_methods', v_tot, 'dim_sales_method: ins='||v_ins||', upd='||v_upd);

  -- PAYMENT METHOD
  v_upd := 0; v_ins := 0; v_tot := 0;

  WITH upd2 AS (
    UPDATE bl_dm.dim_payment_method d
    SET payment_method_name = s.payment_method
    FROM bl_3nf.ce_payment_method s
    WHERE d.source_system = s.source_system
      AND d.source_entity = s.source_entity
      AND d.payment_method_src_id = s.payment_method_src_id
      AND d.payment_method_name IS DISTINCT FROM s.payment_method
    RETURNING 1
  ) SELECT COUNT(*) INTO v_upd FROM upd2;

  WITH ins2 AS (
    INSERT INTO bl_dm.dim_payment_method(payment_method_src_id, payment_method_name, source_system, source_entity)
    SELECT s.payment_method_src_id, s.payment_method, s.source_system, s.source_entity
    FROM bl_3nf.ce_payment_method s
    WHERE NOT EXISTS (
      SELECT 1 FROM bl_dm.dim_payment_method d
      WHERE d.source_system = s.source_system
        AND d.source_entity = s.source_entity
        AND d.payment_method_src_id = s.payment_method_src_id
    )
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins2;

  v_tot := v_upd + v_ins;
  CALL ctl.sp_log('bl_cl.dm_load_methods', v_tot, 'dim_payment_method: ins='||v_ins||', upd='||v_upd);
END $$;

-- 5.5 Customer (SCD2 mirror; already delta-only by design)
CREATE OR REPLACE PROCEDURE bl_cl.dm_load_customer()
LANGUAGE plpgsql
AS $$
DECLARE v_upd BIGINT := 0; v_ins BIGINT := 0; v_tot BIGINT := 0;
BEGIN
  WITH s AS (
    SELECT
      cs.customer_src_id,
      cs.first_name, cs.last_name, cs.full_name,
      bl_cl.fn_date_key(cs.start_dt) AS start_date_key,
      bl_cl.fn_date_key(cs.end_dt)   AS end_date_key,
      cs.is_active AS is_current
    FROM bl_3nf.ce_customer_scd cs
  ),
  upd AS (
    UPDATE bl_dm.dim_customer d
    SET first_name = s.first_name,
        last_name  = s.last_name,
        full_name  = s.full_name,
        is_current = s.is_current
    FROM s
    WHERE d.customer_src_id = s.customer_src_id
      AND d.start_date_key  = s.start_date_key
      AND d.end_date_key    = s.end_date_key
      AND (
        d.first_name IS DISTINCT FROM s.first_name OR
        d.last_name  IS DISTINCT FROM s.last_name  OR
        d.full_name  IS DISTINCT FROM s.full_name  OR
        d.is_current IS DISTINCT FROM s.is_current
      )
    RETURNING 1
  ) SELECT COUNT(*) INTO v_upd FROM upd;

  WITH s AS (
    SELECT
      cs.customer_src_id,
      cs.first_name, cs.last_name, cs.full_name,
      bl_cl.fn_date_key(cs.start_dt) AS start_date_key,
      bl_cl.fn_date_key(cs.end_dt)   AS end_date_key,
      cs.is_active AS is_current
    FROM bl_3nf.ce_customer_scd cs
  ),
  ins AS (
    INSERT INTO bl_dm.dim_customer(customer_src_id, first_name, last_name, full_name,
                                   start_date_key, end_date_key, is_current)
    SELECT s.customer_src_id, s.first_name, s.last_name, s.full_name,
           s.start_date_key, s.end_date_key, s.is_current
    FROM s
    WHERE NOT EXISTS (
      SELECT 1 FROM bl_dm.dim_customer d
      WHERE d.customer_src_id = s.customer_src_id
        AND d.start_date_key  = s.start_date_key
        AND d.end_date_key    = s.end_date_key
    )
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;

  v_tot := v_upd + v_ins;
  CALL ctl.sp_log('bl_cl.dm_load_customer', v_tot, 'dim_customer (SCD2): ins='||v_ins||', upd='||v_upd);
END $$;

-- Orchestrator
CREATE OR REPLACE PROCEDURE bl_cl.dm_load_all()
LANGUAGE plpgsql
AS $$
BEGIN
  CALL bl_cl.dm_load_product();
  CALL bl_cl.dm_load_retailer();
  CALL bl_cl.dm_load_methods();
  CALL bl_cl.dm_load_geography();
  CALL bl_cl.dm_load_customer();
  CALL ctl.sp_log('bl_cl.dm_load_all', 0, 'All DM dimensions loaded');
END $$;

-- =================== quick sanity ===================
-- CALL bl_cl.dm_build_date('2010-01-01','2016-12-31'); -- optional calendar prefill
-- CALL bl_cl.dm_load_all();
-- CALL bl_cl.dm_load_all();  -- 2nd run => 0 deltas



SELECT * FROM ctl.etl_log ORDER BY log_id DESC LIMIT 30;

SELECT COUNT(*) AS n_prod   FROM bl_dm.dim_product;
SELECT COUNT(*) AS n_retail FROM bl_dm.dim_retailer;
SELECT COUNT(*) AS n_geo    FROM bl_dm.dim_geography;
SELECT COUNT(*) AS n_sm     FROM bl_dm.dim_sales_method;
SELECT COUNT(*) AS n_pm     FROM bl_dm.dim_payment_method;
SELECT COUNT(*) AS n_cust   FROM bl_dm.dim_customer;

SELECT * FROM bl_dm.dim_product   LIMIT 10;
SELECT * FROM bl_dm.dim_geography LIMIT 10;
SELECT * FROM bl_dm.dim_customer  ORDER BY customer_src_id, start_date_key LIMIT 20;
