-- ===========================
-- Sprint 2 • Task 1 • Loaders
-- ===========================
SET search_path = public, ctl, bl_cl, bl_3nf, sa_instore, sa_online;

-- --- Schemas & logging -----------------------------------------------
CREATE SCHEMA IF NOT EXISTS ctl;
CREATE SCHEMA IF NOT EXISTS bl_cl;

CREATE TABLE IF NOT EXISTS ctl.etl_log (
  log_id         BIGSERIAL PRIMARY KEY,
  log_ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
  procedure_name TEXT NOT NULL,
  rows_affected  BIGINT NOT NULL,
  message        TEXT
);

CREATE OR REPLACE PROCEDURE ctl.sp_log(p_proc TEXT, p_rows BIGINT, p_msg TEXT)
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ctl.etl_log(procedure_name, rows_affected, message)
  VALUES (p_proc, COALESCE(p_rows,0), p_msg);
END $$;


--CREATE INDEX IF NOT EXISTS ix_cat_src    ON bl_3nf.ce_category (source_system, source_entity, category_src_id);
--CREATE INDEX IF NOT EXISTS ix_prod_src   ON bl_3nf.ce_product  (source_system, source_entity, product_src_id);
--CREATE INDEX IF NOT EXISTS ix_retail_src ON bl_3nf.ce_retailer (source_system, source_entity, retailer_src_id);
--CREATE INDEX IF NOT EXISTS ix_sm_src     ON bl_3nf.ce_sales_method (source_system, source_entity, sales_method_src_id);
--CREATE INDEX IF NOT EXISTS ix_pm_src     ON bl_3nf.ce_payment_method (source_system, source_entity, payment_method_src_id);
--CREATE INDEX IF NOT EXISTS ix_cust_active ON bl_3nf.ce_customer_scd (customer_src_id) WHERE is_active;

-- =====================================================================
-- 1) CATEGORY loader (INSTORE + ONLINE)
-- =====================================================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_load_ce_category()
LANGUAGE plpgsql
AS $$
DECLARE
  r        record;        -- {src_sys, src_ent}
  v_ins    bigint := 0;   -- per-iteration count
  v_total  bigint := 0;   -- total count
BEGIN
  FOR r IN
    SELECT 'SA_INSTORE'::text AS src_sys, 'SRC_INSTORE'::text AS src_ent
    UNION ALL
    SELECT 'SA_ONLINE'::text,  'SRC_ONLINE'::text
  LOOP
    IF r.src_sys = 'SA_INSTORE' THEN
      WITH ins AS (
        INSERT INTO bl_3nf.ce_category (category_src_id, category, source_system, source_entity)
        SELECT DISTINCT
               s.product_category_id,
               COALESCE(TRIM(s.product_category),'N/A'),
               r.src_sys, r.src_ent
        FROM sa_instore.src_instore_sales s
        ON CONFLICT (source_system, source_entity, category_src_id) DO NOTHING
        RETURNING 1
      ) SELECT COUNT(*) INTO v_ins FROM ins;
    ELSE
      WITH ins AS (
        INSERT INTO bl_3nf.ce_category (category_src_id, category, source_system, source_entity)
        SELECT DISTINCT
               s.product_category_id,
               COALESCE(TRIM(s.product_category),'N/A'),
               r.src_sys, r.src_ent
        FROM sa_online.src_online_sales s
        ON CONFLICT (source_system, source_entity, category_src_id) DO NOTHING
        RETURNING 1
      ) SELECT COUNT(*) INTO v_ins FROM ins;
    END IF;

    v_total := v_total + COALESCE(v_ins,0);
  END LOOP;

  CALL ctl.sp_log('bl_cl.sp_load_ce_category', v_total, 'Categories loaded');
EXCEPTION WHEN OTHERS THEN
  CALL ctl.sp_log('bl_cl.sp_load_ce_category', 0, 'ERROR: '||SQLERRM);
  RAISE;
END;
$$;


-- =====================================================================
-- 2) PRODUCT loader (INSTORE + ONLINE)
-- =====================================================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_load_ce_product()
LANGUAGE plpgsql
AS $$
DECLARE v_ins BIGINT := 0; v_tot BIGINT := 0;
BEGIN
  -- INSTORE
  WITH ins AS (
    INSERT INTO bl_3nf.ce_product (product_src_id, product_name, category_id, source_system, source_entity)
    SELECT DISTINCT s.product_id,
           COALESCE(TRIM(s.product),'N/A'),
           c.category_id,
           'SA_INSTORE','SRC_INSTORE'
    FROM sa_instore.src_instore_sales s
    JOIN bl_3nf.ce_category c
      ON c.source_system='SA_INSTORE' AND c.source_entity='SRC_INSTORE'
     AND c.category_src_id = s.product_category_id
    ON CONFLICT (source_system, source_entity, product_src_id) DO NOTHING
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;
  v_tot := v_tot + COALESCE(v_ins,0);

  -- ONLINE
  WITH ins AS (
    INSERT INTO bl_3nf.ce_product (product_src_id, product_name, category_id, source_system, source_entity)
    SELECT DISTINCT s.product_id,
           COALESCE(TRIM(s.product),'N/A'),
           c.category_id,
           'SA_ONLINE','SRC_ONLINE'
    FROM sa_online.src_online_sales s
    JOIN bl_3nf.ce_category c
      ON c.source_system='SA_ONLINE' AND c.source_entity='SRC_ONLINE'
     AND c.category_src_id = s.product_category_id
    ON CONFLICT (source_system, source_entity, product_src_id) DO NOTHING
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;
  v_tot := v_tot + COALESCE(v_ins,0);

  CALL ctl.sp_log('bl_cl.sp_load_ce_product', v_tot, 'Products loaded');
EXCEPTION WHEN OTHERS THEN
  CALL ctl.sp_log('bl_cl.sp_load_ce_product', 0, 'ERROR: '||SQLERRM);
  RAISE;
END;
$$;

-- =====================================================================
-- 3) RETAILER loader + RETURNS TABLE function
-- =====================================================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_load_ce_retailer()
LANGUAGE plpgsql
AS $$
DECLARE v_ins BIGINT := 0; v_tot BIGINT := 0;
BEGIN
  -- INSTORE
  WITH ins AS (
    INSERT INTO bl_3nf.ce_retailer (retailer_src_id, retailer, source_system, source_entity)
    SELECT DISTINCT TRIM(COALESCE(s.retailer,'N/A')),
           TRIM(COALESCE(s.retailer,'N/A')),
           'SA_INSTORE','SRC_INSTORE'
    FROM sa_instore.src_instore_sales s
    WHERE TRIM(COALESCE(s.retailer,'')) <> ''
    ON CONFLICT (source_system, source_entity, retailer_src_id) DO NOTHING
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;
  v_tot := v_tot + COALESCE(v_ins,0);

  -- ONLINE
  WITH ins AS (
    INSERT INTO bl_3nf.ce_retailer (retailer_src_id, retailer, source_system, source_entity)
    SELECT DISTINCT TRIM(COALESCE(s.retailer,'N/A')),
           TRIM(COALESCE(s.retailer,'N/A')),
           'SA_ONLINE','SRC_ONLINE'
    FROM sa_online.src_online_sales s
    WHERE TRIM(COALESCE(s.retailer,'')) <> ''
    ON CONFLICT (source_system, source_entity, retailer_src_id) DO NOTHING
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;
  v_tot := v_tot + COALESCE(v_ins,0);

  CALL ctl.sp_log('bl_cl.sp_load_ce_retailer', v_tot, 'Retailers loaded');
EXCEPTION WHEN OTHERS THEN
  CALL ctl.sp_log('bl_cl.sp_load_ce_retailer', 0, 'ERROR: '||SQLERRM);
  RAISE;
END;
$$;


CREATE OR REPLACE FUNCTION bl_cl.fn_load_ce_retailer()
RETURNS TABLE (action text, ret_src_id text, src_system text)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  WITH new_instore AS (
    INSERT INTO bl_3nf.ce_retailer (retailer_src_id, retailer, source_system, source_entity)
    SELECT DISTINCT TRIM(COALESCE(s.retailer,'N/A')),
           TRIM(COALESCE(s.retailer,'N/A')),
           'SA_INSTORE','SRC_INSTORE'
    FROM sa_instore.src_instore_sales s
    WHERE TRIM(COALESCE(s.retailer,'')) <> ''
    ON CONFLICT (source_system, source_entity, retailer_src_id) DO NOTHING
    RETURNING bl_3nf.ce_retailer.retailer_src_id
  ),
  new_online AS (
    INSERT INTO bl_3nf.ce_retailer (retailer_src_id, retailer, source_system, source_entity)
    SELECT DISTINCT TRIM(COALESCE(s.retailer,'N/A')),
           TRIM(COALESCE(s.retailer,'N/A')),
           'SA_ONLINE','SRC_ONLINE'
    FROM sa_online.src_online_sales s
    WHERE TRIM(COALESCE(s.retailer,'')) <> ''
    ON CONFLICT (source_system, source_entity, retailer_src_id) DO NOTHING
    RETURNING bl_3nf.ce_retailer.retailer_src_id
  )
  SELECT 'INSERT', ni.retailer_src_id, 'SA_INSTORE' FROM new_instore ni
  UNION ALL
  SELECT 'INSERT', no.retailer_src_id, 'SA_ONLINE'  FROM new_online  no;

  PERFORM ctl.sp_log('bl_cl.fn_load_ce_retailer',
                     (SELECT COUNT(*) FROM bl_3nf.ce_retailer),
                     'Retailer function run');
END;
$$;

-- =====================================================================
-- 4) METHODS loader (sales/payment)
-- =====================================================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_load_ce_methods()
LANGUAGE plpgsql
AS $$
DECLARE v_ins BIGINT := 0; v_tot BIGINT := 0;
BEGIN
  -- INSTORE
  WITH ins1 AS (
    INSERT INTO bl_3nf.ce_sales_method (sales_method_src_id, sales_method, source_system, source_entity)
    SELECT DISTINCT s.sales_method_id, COALESCE(TRIM(s.sales_method),'N/A'), 'SA_INSTORE','SRC_INSTORE'
    FROM sa_instore.src_instore_sales s
    ON CONFLICT (source_system, source_entity, sales_method_src_id) DO NOTHING
    RETURNING 1
  ), ins2 AS (
    INSERT INTO bl_3nf.ce_payment_method (payment_method_src_id, payment_method, source_system, source_entity)
    SELECT DISTINCT s.payment_method_id, COALESCE(TRIM(s.payment_method),'N/A'), 'SA_INSTORE','SRC_INSTORE'
    FROM sa_instore.src_instore_sales s
    ON CONFLICT (source_system, source_entity, payment_method_src_id) DO NOTHING
    RETURNING 1
  ) SELECT (SELECT COUNT(*) FROM ins1) + (SELECT COUNT(*) FROM ins2) INTO v_ins;
  v_tot := v_tot + COALESCE(v_ins,0);

  -- ONLINE
  WITH ins1 AS (
    INSERT INTO bl_3nf.ce_sales_method (sales_method_src_id, sales_method, source_system, source_entity)
    SELECT DISTINCT s.sales_method_id, COALESCE(TRIM(s.sales_method),'N/A'), 'SA_ONLINE','SRC_ONLINE'
    FROM sa_online.src_online_sales s
    ON CONFLICT (source_system, source_entity, sales_method_src_id) DO NOTHING
    RETURNING 1
  ), ins2 AS (
    INSERT INTO bl_3nf.ce_payment_method (payment_method_src_id, payment_method, source_system, source_entity)
    SELECT DISTINCT s.payment_method_id, COALESCE(TRIM(s.payment_method),'N/A'), 'SA_ONLINE','SRC_ONLINE'
    FROM sa_online.src_online_sales s
    ON CONFLICT (source_system, source_entity, payment_method_src_id) DO NOTHING
    RETURNING 1
  ) SELECT (SELECT COUNT(*) FROM ins1) + (SELECT COUNT(*) FROM ins2) INTO v_ins;
  v_tot := v_tot + COALESCE(v_ins,0);

  CALL ctl.sp_log('bl_cl.sp_load_ce_methods', v_tot, 'Sales/Payment methods loaded');
EXCEPTION WHEN OTHERS THEN
  CALL ctl.sp_log('bl_cl.sp_load_ce_methods', 0, 'ERROR: '||SQLERRM);
  RAISE;
END;
$$;

-- =====================================================================
-- 5) GEO loader (instore only)
-- =====================================================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_load_geo()
LANGUAGE plpgsql
AS $$
DECLARE v_ins BIGINT := 0;
BEGIN
  WITH r AS (
    INSERT INTO bl_3nf.ce_region(region_id, region_src_id, region_name, source_system, source_entity)
    SELECT DISTINCT COALESCE(s.region_id,-1), s.region_id, COALESCE(TRIM(s.region),'N/A'), 'SA_INSTORE','SRC_INSTORE'
    FROM sa_instore.src_instore_sales s
    ON CONFLICT (region_id) DO NOTHING
    RETURNING 1
  ), st AS (
    INSERT INTO bl_3nf.ce_state(state_id, state_src_id, state_name, region_id, source_system, source_entity)
    SELECT DISTINCT COALESCE(s.state_id,-1), s.state_id, COALESCE(TRIM(s.state),'N/A'),
           COALESCE(s.region_id,-1), 'SA_INSTORE','SRC_INSTORE'
    FROM sa_instore.src_instore_sales s
    ON CONFLICT (state_id) DO NOTHING
    RETURNING 1
  ), c AS (
    INSERT INTO bl_3nf.ce_city(city_id, city_src_id, city_name, state_id, source_system, source_entity)
    SELECT DISTINCT COALESCE(s.city_id,-1), s.city_id, COALESCE(TRIM(s.city),'N/A'),
           COALESCE(s.state_id,-1), 'SA_INSTORE','SRC_INSTORE'
    FROM sa_instore.src_instore_sales s
    ON CONFLICT (city_id) DO NOTHING
    RETURNING 1
  )
  SELECT (SELECT COUNT(*) FROM r)+(SELECT COUNT(*) FROM st)+(SELECT COUNT(*) FROM c) INTO v_ins;

  CALL ctl.sp_log('bl_cl.sp_load_geo', v_ins, 'Geo loaded');
EXCEPTION WHEN OTHERS THEN
  CALL ctl.sp_log('bl_cl.sp_load_geo', 0, 'ERROR: '||SQLERRM);
  RAISE;
END;
$$;

-- =====================================================================
-- 6) CUSTOMER SCD2 loader
-- =====================================================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_load_ce_customer_scd()
LANGUAGE plpgsql
AS $$
DECLARE v_changes BIGINT := 0; v_tot BIGINT := 0;
BEGIN
  -- INSTORE: close changed
  WITH x AS (
    SELECT DISTINCT
      COALESCE(customer_id,0) AS customer_src_id,
      TRIM(COALESCE(customer_firstname,'N/A')) AS fn,
      TRIM(COALESCE(customer_lastname ,'N/A')) AS ln
    FROM sa_instore.src_instore_sales
  ), closed AS (
    UPDATE bl_3nf.ce_customer_scd cur
    SET end_dt = CURRENT_DATE - INTERVAL '1 day',
        is_active = FALSE,
        update_dt = NOW()
    FROM x
    WHERE cur.customer_src_id = x.customer_src_id
      AND cur.is_active = TRUE
      AND (cur.first_name <> x.fn OR cur.last_name <> x.ln)
    RETURNING 1
  ) SELECT COUNT(*) FROM closed INTO v_changes;
  v_tot := v_tot + COALESCE(v_changes,0);

  -- INSTORE: insert new current
  WITH x AS (
    SELECT DISTINCT
      COALESCE(customer_id,0) AS customer_src_id,
      TRIM(COALESCE(customer_firstname,'N/A')) AS fn,
      TRIM(COALESCE(customer_lastname ,'N/A')) AS ln
    FROM sa_instore.src_instore_sales
  ), ins AS (
    INSERT INTO bl_3nf.ce_customer_scd (
      customer_src_id, first_name, last_name, full_name,
      start_dt, end_dt, is_active, source_system, source_entity
    )
    SELECT
      x.customer_src_id, x.fn, x.ln,
      CONCAT_WS(' ', NULLIF(x.fn,'N/A'), NULLIF(x.ln,'N/A')),
      CURRENT_DATE, DATE '9999-12-31', TRUE, 'SA_INSTORE','SRC_INSTORE'
    FROM x
    WHERE NOT EXISTS (
      SELECT 1 FROM bl_3nf.ce_customer_scd t
      WHERE t.customer_src_id = x.customer_src_id AND t.is_active = TRUE
    )
    RETURNING 1
  ) SELECT COUNT(*) FROM ins INTO v_changes;
  v_tot := v_tot + COALESCE(v_changes,0);

  -- ONLINE: close changed
  WITH x AS (
    SELECT DISTINCT
      COALESCE(customer_id,0) AS customer_src_id,
      TRIM(COALESCE(customer_firstname,'N/A')) AS fn,
      TRIM(COALESCE(customer_lastname ,'N/A')) AS ln
    FROM sa_online.src_online_sales
  ), closed AS (
    UPDATE bl_3nf.ce_customer_scd cur
    SET end_dt = CURRENT_DATE - INTERVAL '1 day',
        is_active = FALSE,
        update_dt = NOW()
    FROM x
    WHERE cur.customer_src_id = x.customer_src_id
      AND cur.is_active = TRUE
      AND (cur.first_name <> x.fn OR cur.last_name <> x.ln)
    RETURNING 1
  ) SELECT COUNT(*) FROM closed INTO v_changes;
  v_tot := v_tot + COALESCE(v_changes,0);

  -- ONLINE: insert new current
  WITH x AS (
    SELECT DISTINCT
      COALESCE(customer_id,0) AS customer_src_id,
      TRIM(COALESCE(customer_firstname,'N/A')) AS fn,
      TRIM(COALESCE(customer_lastname ,'N/A')) AS ln
    FROM sa_online.src_online_sales
  ), ins AS (
    INSERT INTO bl_3nf.ce_customer_scd (
      customer_src_id, first_name, last_name, full_name,
      start_dt, end_dt, is_active, source_system, source_entity
    )
    SELECT
      x.customer_src_id, x.fn, x.ln,
      CONCAT_WS(' ', NULLIF(x.fn,'N/A'), NULLIF(x.ln,'N/A')),
      CURRENT_DATE, DATE '9999-12-31', TRUE, 'SA_ONLINE','SRC_ONLINE'
    FROM x
    WHERE NOT EXISTS (
      SELECT 1 FROM bl_3nf.ce_customer_scd t
      WHERE t.customer_src_id = x.customer_src_id AND t.is_active = TRUE
    )
    RETURNING 1
  ) SELECT COUNT(*) FROM ins INTO v_changes;
  v_tot := v_tot + COALESCE(v_changes,0);

  CALL ctl.sp_log('bl_cl.sp_load_ce_customer_scd', v_tot, 'Customer SCD2 processed');
EXCEPTION WHEN OTHERS THEN
  CALL ctl.sp_log('bl_cl.sp_load_ce_customer_scd', 0, 'ERROR: '||SQLERRM);
  RAISE;
END;
$$;

-- =====================================================================
-- 7) SALES loader (INSTORE + ONLINE)
-- =====================================================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_load_ce_sales()
LANGUAGE plpgsql
AS $$
DECLARE v_ins BIGINT := 0; v_tot BIGINT := 0;
BEGIN
  -- INSTORE
  WITH ins AS (
    INSERT INTO bl_3nf.ce_sales (
      sale_src_id, invoice_ts,
      customer_id, product_id, retailer_id, city_id, sales_method_id, payment_method_id,
      units_sold, price_per_unit, total_sales, operating_profit, operating_margin,
      avg_unit_price, profit_per_unit, is_high_margin,
      source_system, source_entity
    )
    SELECT
      s.source_id,
      s.invoice_ts,
      COALESCE((SELECT cs.customer_id
                FROM bl_3nf.ce_customer_scd cs
                WHERE cs.customer_src_id = COALESCE(s.customer_id,0)
                  AND cs.is_active = TRUE
                LIMIT 1), -1),
      COALESCE(p.product_id,-1),
      COALESCE(r.retailer_id,-1),
      COALESCE(c.city_id,-1),
      COALESCE(sm.sales_method_id,-1),
      COALESCE(pm.payment_method_id,-1),
      COALESCE(s.units_sold,0),
      COALESCE(s.price_per_unit,0),
      COALESCE(s.total_sales,0),
      s.operating_profit,
      s.operating_margin,
      NULLIF(s.total_sales,0)::NUMERIC / NULLIF(s.units_sold,0)::NUMERIC,
      CASE WHEN s.units_sold > 0 THEN (s.total_sales - COALESCE(s.operating_profit,0)) / s.units_sold END,
      CASE WHEN s.operating_margin >= 0.30 THEN TRUE ELSE FALSE END,
      'SA_INSTORE','SRC_INSTORE'
    FROM sa_instore.src_instore_sales s
    LEFT JOIN bl_3nf.ce_product p
      ON p.source_system='SA_INSTORE' AND p.source_entity='SRC_INSTORE'
     AND p.product_src_id = s.product_id
    LEFT JOIN bl_3nf.ce_retailer r
      ON r.source_system='SA_INSTORE' AND r.source_entity='SRC_INSTORE'
     AND r.retailer_src_id = TRIM(COALESCE(s.retailer,'N/A'))
    LEFT JOIN bl_3nf.ce_sales_method sm
      ON sm.source_system='SA_INSTORE' AND sm.source_entity='SRC_INSTORE'
     AND sm.sales_method_src_id = s.sales_method_id
    LEFT JOIN bl_3nf.ce_payment_method pm
      ON pm.source_system='SA_INSTORE' AND pm.source_entity='SRC_INSTORE'
     AND pm.payment_method_src_id = s.payment_method_id
    LEFT JOIN bl_3nf.ce_city c
      ON c.city_id = s.city_id
    ON CONFLICT ON CONSTRAINT uq_ce_sales_nk DO NOTHING
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;
  v_tot := v_tot + COALESCE(v_ins,0);

  -- ONLINE (no geo)
  WITH ins AS (
    INSERT INTO bl_3nf.ce_sales (
      sale_src_id, invoice_ts,
      customer_id, product_id, retailer_id, city_id, sales_method_id, payment_method_id,
      units_sold, price_per_unit, total_sales, operating_profit, operating_margin,
      avg_unit_price, profit_per_unit, is_high_margin,
      source_system, source_entity
    )
    SELECT
      s.source_id,
      s.invoice_ts,
      COALESCE((SELECT cs.customer_id
                FROM bl_3nf.ce_customer_scd cs
                WHERE cs.customer_src_id = COALESCE(s.customer_id,0)
                  AND cs.is_active = TRUE
                LIMIT 1), -1),
      COALESCE(p.product_id,-1),
      COALESCE(r.retailer_id,-1),
      -1,
      COALESCE(sm.sales_method_id,-1),
      COALESCE(pm.payment_method_id,-1),
      COALESCE(s.units_sold,0),
      COALESCE(s.price_per_unit,0),
      COALESCE(s.total_sales,0),
      s.operating_profit,
      s.operating_margin,
      NULLIF(s.total_sales,0)::NUMERIC / NULLIF(s.units_sold,0)::NUMERIC,
      CASE WHEN s.units_sold > 0 THEN (s.total_sales - COALESCE(s.operating_profit,0)) / s.units_sold END,
      CASE WHEN s.operating_margin >= 0.30 THEN TRUE ELSE FALSE END,
      'SA_ONLINE','SRC_ONLINE'
    FROM sa_online.src_online_sales s
    LEFT JOIN bl_3nf.ce_product p
      ON p.source_system='SA_ONLINE' AND p.source_entity='SRC_ONLINE'
     AND p.product_src_id = s.product_id
    LEFT JOIN bl_3nf.ce_retailer r
      ON r.source_system='SA_ONLINE' AND r.source_entity='SRC_ONLINE'
     AND r.retailer_src_id = TRIM(COALESCE(s.retailer,'N/A'))
    LEFT JOIN bl_3nf.ce_sales_method sm
      ON sm.source_system='SA_ONLINE' AND sm.source_entity='SRC_ONLINE'
     AND sm.sales_method_src_id = s.sales_method_id
    LEFT JOIN bl_3nf.ce_payment_method pm
      ON pm.source_system='SA_ONLINE' AND pm.source_entity='SRC_ONLINE'
     AND pm.payment_method_src_id = s.payment_method_id
    ON CONFLICT ON CONSTRAINT uq_ce_sales_nk DO NOTHING
    RETURNING 1
  ) SELECT COUNT(*) INTO v_ins FROM ins;
  v_tot := v_tot + COALESCE(v_ins,0);

  CALL ctl.sp_log('bl_cl.sp_load_ce_sales', v_tot, 'Sales loaded');
EXCEPTION WHEN OTHERS THEN
  CALL ctl.sp_log('bl_cl.sp_load_ce_sales', 0, 'ERROR: '||SQLERRM);
  RAISE;
END;
$$;


-- =====================================================================
-- 8) Orchestrated calls
-- =====================================================================
 BEGIN;
   CALL bl_cl.sp_load_ce_category();
   CALL bl_cl.sp_load_ce_product();
   CALL bl_cl.sp_load_ce_retailer();
   CALL bl_cl.sp_load_ce_methods();
   CALL bl_cl.sp_load_geo();
   CALL bl_cl.sp_load_ce_customer_scd();
   CALL bl_cl.sp_load_ce_sales();
 COMMIT;

-- =====================================================================
-- 9) Verification queries
-- =====================================================================
 SELECT * FROM ctl.etl_log ORDER BY log_id DESC LIMIT 20;
 SELECT COUNT(*) AS n_sales FROM bl_3nf.ce_sales;
 SELECT * FROM bl_3nf.ce_category   LIMIT 10;
 SELECT * FROM bl_3nf.ce_product    LIMIT 10;
 SELECT * FROM bl_3nf.ce_retailer   LIMIT 10;
 SELECT * FROM bl_3nf.ce_customer_scd WHERE is_active LIMIT 300;
