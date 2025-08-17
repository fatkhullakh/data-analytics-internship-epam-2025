SELECT source_system, source_entity, COUNT(*) FROM bl_3nf.ce_sales GROUP BY 1,2;


-- INSTORE categories
INSERT INTO bl_3nf.ce_category (category_src_id, category, source_system, source_entity)
SELECT DISTINCT s.product_category_id, COALESCE(TRIM(s.product_category),'N/A'), 'SA_INSTORE','SRC_INSTORE'
FROM sa_instore.src_instore_sales s
ON CONFLICT (source_system, source_entity, category_src_id) DO NOTHING;


-- ONLINE categories
INSERT INTO bl_3nf.ce_category (category_src_id, category, source_system, source_entity)
SELECT DISTINCT s.product_category_id, COALESCE(TRIM(s.product_category),'N/A'), 'SA_ONLINE','SRC_ONLINE'
FROM sa_online.src_online_sales s
ON CONFLICT (source_system, source_entity, category_src_id) DO NOTHING;


-- INSTORE products
INSERT INTO bl_3nf.ce_product (product_src_id, product_name, category_id, source_system, source_entity)
SELECT DISTINCT
  s.product_id,
  COALESCE(TRIM(s.product),'N/A'),
  c.category_id,                      -- resolved by source + src_id
  'SA_INSTORE','SRC_INSTORE'
FROM sa_instore.src_instore_sales s
JOIN bl_3nf.ce_category c
  ON c.source_system='SA_INSTORE'
 AND c.source_entity='SRC_INSTORE'
 AND c.category_src_id = s.product_category_id
ON CONFLICT (source_system, source_entity, product_src_id) DO NOTHING;


-- ONLINE products
INSERT INTO bl_3nf.ce_product (product_src_id, product_name, category_id, source_system, source_entity)
SELECT DISTINCT
  s.product_id,
  COALESCE(TRIM(s.product),'N/A'),
  c.category_id,
  'SA_ONLINE','SRC_ONLINE'
FROM sa_online.src_online_sales s
JOIN bl_3nf.ce_category c
  ON c.source_system='SA_ONLINE'
 AND c.source_entity='SRC_ONLINE'
 AND c.category_src_id = s.product_category_id
ON CONFLICT (source_system, source_entity, product_src_id) DO NOTHING;



-- INSTORE retailers
INSERT INTO bl_3nf.ce_retailer (retailer_src_id, retailer, source_system, source_entity)
SELECT DISTINCT TRIM(COALESCE(s.retailer,'N/A')),
       TRIM(COALESCE(s.retailer,'N/A')),
       'SA_INSTORE','SRC_INSTORE'
FROM sa_instore.src_instore_sales s
WHERE TRIM(COALESCE(s.retailer,'')) <> ''
ON CONFLICT (source_system, source_entity, retailer_src_id) DO NOTHING;


-- ONLINE retailers
INSERT INTO bl_3nf.ce_retailer (retailer_src_id, retailer, source_system, source_entity)
SELECT DISTINCT TRIM(COALESCE(s.retailer,'N/A')),
       TRIM(COALESCE(s.retailer,'N/A')),
       'SA_ONLINE','SRC_ONLINE'
FROM sa_online.src_online_sales s
WHERE TRIM(COALESCE(s.retailer,'')) <> ''
ON CONFLICT (source_system, source_entity, retailer_src_id) DO NOTHING;




-- INSTORE
INSERT INTO bl_3nf.ce_sales_method (sales_method_src_id, sales_method, source_system, source_entity)
SELECT DISTINCT s.sales_method_id, COALESCE(TRIM(s.sales_method),'N/A'), 'SA_INSTORE','SRC_INSTORE'
FROM sa_instore.src_instore_sales s
ON CONFLICT (source_system, source_entity, sales_method_src_id) DO NOTHING;


INSERT INTO bl_3nf.ce_payment_method (payment_method_src_id, payment_method, source_system, source_entity)
SELECT DISTINCT s.payment_method_id, COALESCE(TRIM(s.payment_method),'N/A'), 'SA_INSTORE','SRC_INSTORE'
FROM sa_instore.src_instore_sales s
ON CONFLICT (source_system, source_entity, payment_method_src_id) DO NOTHING;


-- ONLINE
INSERT INTO bl_3nf.ce_sales_method (sales_method_src_id, sales_method, source_system, source_entity)
SELECT DISTINCT s.sales_method_id, COALESCE(TRIM(s.sales_method),'N/A'), 'SA_ONLINE','SRC_ONLINE'
FROM sa_online.src_online_sales s
ON CONFLICT (source_system, source_entity, sales_method_src_id) DO NOTHING;


INSERT INTO bl_3nf.ce_payment_method (payment_method_src_id, payment_method, source_system, source_entity)
SELECT DISTINCT s.payment_method_id, COALESCE(TRIM(s.payment_method),'N/A'), 'SA_ONLINE','SRC_ONLINE'
FROM sa_online.src_online_sales s
ON CONFLICT (source_system, source_entity, payment_method_src_id) DO NOTHING;



-- Geography only exists in INSTORE
-- REGION (INSTORE)
INSERT INTO bl_3nf.ce_region(region_id, region_src_id, region_name, source_system, source_entity)
SELECT DISTINCT COALESCE(s.region_id,-1), s.region_id, COALESCE(TRIM(s.region),'N/A'), 'SA_INSTORE','SRC_INSTORE'
FROM sa_instore.src_instore_sales s
ON CONFLICT (region_id) DO NOTHING;

-- STATE (INSTORE)
INSERT INTO bl_3nf.ce_state(state_id, state_src_id, state_name, region_id, source_system, source_entity)
SELECT DISTINCT COALESCE(s.state_id,-1), s.state_id, COALESCE(TRIM(s.state),'N/A'), COALESCE(s.region_id,-1), 'SA_INSTORE','SRC_INSTORE'
FROM sa_instore.src_instore_sales s
ON CONFLICT (state_id) DO NOTHING;

-- CITY (INSTORE)
INSERT INTO bl_3nf.ce_city(city_id, city_src_id, city_name, state_id, source_system, source_entity)
SELECT DISTINCT COALESCE(s.city_id,-1), s.city_id, COALESCE(TRIM(s.city),'N/A'), COALESCE(s.state_id,-1), 'SA_INSTORE','SRC_INSTORE'
FROM sa_instore.src_instore_sales s
ON CONFLICT (city_id) DO NOTHING;    



-- CUSTOMER (INSTORE)
INSERT INTO bl_3nf.ce_customer_scd (
  customer_src_id, first_name, last_name, full_name,
  start_dt, end_dt, is_active, source_system, source_entity
)
SELECT DISTINCT
  COALESCE(s.customer_id,0),
  TRIM(COALESCE(s.customer_firstname,'N/A')),
  TRIM(COALESCE(s.customer_lastname ,'N/A')),
  CONCAT_WS(' ',
    NULLIF(TRIM(COALESCE(s.customer_firstname,'N/A')),'N/A'),
    NULLIF(TRIM(COALESCE(s.customer_lastname ,'N/A')),'N/A')
  ),
  CURRENT_DATE, DATE '9999-12-31', TRUE, 'SA_INSTORE','SRC_INSTORE'
FROM sa_instore.src_instore_sales s
WHERE NOT EXISTS (
  SELECT 1 FROM bl_3nf.ce_customer_scd t
  WHERE t.customer_src_id = COALESCE(s.customer_id,0) AND t.is_active = TRUE
);


UPDATE bl_3nf.ce_customer_scd cur
SET end_dt    = CURRENT_DATE - INTERVAL '1 day',
    is_active = FALSE,
    update_dt = NOW()
FROM (
  SELECT DISTINCT
    COALESCE(customer_id,0) AS customer_src_id,
    TRIM(COALESCE(customer_firstname,'N/A')) AS fn,
    TRIM(COALESCE(customer_lastname ,'N/A')) AS ln
  FROM sa_instore.src_instore_sales
) x
WHERE cur.customer_src_id = x.customer_src_id
  AND cur.is_active = TRUE
  AND (cur.first_name <> x.fn OR cur.last_name <> x.ln);


INSERT INTO bl_3nf.ce_customer_scd (
  customer_src_id, first_name, last_name, full_name,
  start_dt, end_dt, is_active, source_system, source_entity
)
SELECT
  x.customer_src_id, x.fn, x.ln,
  CONCAT_WS(' ', NULLIF(x.fn,'N/A'), NULLIF(x.ln,'N/A')),
  CURRENT_DATE, DATE '9999-12-31', TRUE, 'SA_INSTORE','SRC_INSTORE'
FROM (
  SELECT DISTINCT
    COALESCE(customer_id,0) AS customer_src_id,
    TRIM(COALESCE(customer_firstname,'N/A')) AS fn,
    TRIM(COALESCE(customer_lastname ,'N/A')) AS ln
  FROM sa_instore.src_instore_sales
) x
WHERE NOT EXISTS (
  SELECT 1 FROM bl_3nf.ce_customer_scd t
  WHERE t.customer_src_id = x.customer_src_id AND t.is_active = TRUE
);





--CUSTOMER(ONLINE)
INSERT INTO bl_3nf.ce_customer_scd (
  customer_src_id, first_name, last_name, full_name,
  start_dt, end_dt, is_active, source_system, source_entity
)
SELECT DISTINCT
  COALESCE(s.customer_id,0),
  TRIM(COALESCE(s.customer_firstname,'N/A')),
  TRIM(COALESCE(s.customer_lastname ,'N/A')),
  CONCAT_WS(' ',
    NULLIF(TRIM(COALESCE(s.customer_firstname,'N/A')),'N/A'),
    NULLIF(TRIM(COALESCE(s.customer_lastname ,'N/A')),'N/A')
  ),
  CURRENT_DATE, DATE '9999-12-31', TRUE, 'SA_ONLINE','SRC_ONLINE'
FROM sa_online.src_online_sales s
WHERE NOT EXISTS (
  SELECT 1 FROM bl_3nf.ce_customer_scd t
  WHERE t.customer_src_id = COALESCE(s.customer_id,0) AND t.is_active = TRUE
);


UPDATE bl_3nf.ce_customer_scd cur
SET end_dt    = CURRENT_DATE - INTERVAL '1 day',
    is_active = FALSE,
    update_dt = NOW()
FROM (
  SELECT DISTINCT
    COALESCE(customer_id,0) AS customer_src_id,
    TRIM(COALESCE(customer_firstname,'N/A')) AS fn,
    TRIM(COALESCE(customer_lastname ,'N/A')) AS ln
  FROM sa_online.src_online_sales
) x
WHERE cur.customer_src_id = x.customer_src_id
  AND cur.is_active = TRUE
  AND (cur.first_name <> x.fn OR cur.last_name <> x.ln);


INSERT INTO bl_3nf.ce_customer_scd (
  customer_src_id, first_name, last_name, full_name,
  start_dt, end_dt, is_active, source_system, source_entity
)
SELECT
  x.customer_src_id, x.fn, x.ln,
  CONCAT_WS(' ', NULLIF(x.fn,'N/A'), NULLIF(x.ln,'N/A')),
  CURRENT_DATE, DATE '9999-12-31', TRUE, 'SA_ONLINE','SRC_ONLINE'
FROM (
  SELECT DISTINCT
    COALESCE(customer_id,0) AS customer_src_id,
    TRIM(COALESCE(customer_firstname,'N/A')) AS fn,
    TRIM(COALESCE(customer_lastname ,'N/A')) AS ln
  FROM sa_online.src_online_sales
) x
WHERE NOT EXISTS (
  SELECT 1 FROM bl_3nf.ce_customer_scd t
  WHERE t.customer_src_id = x.customer_src_id AND t.is_active = TRUE
);


-----------------------------------------------------------------------------------------------------------------------------------

SELECT COUNT(*) FROM bl_3nf.ce_sales

SELECT * FROM bl_3nf.ce_sales

-----------------------------------------------------------------------------------------------------------------------------------

INSERT INTO bl_3nf.ce_sales (
  sale_src_id, invoice_ts,
  customer_id, product_id, retailer_id, city_id, sales_method_id, payment_method_id,
  units_sold, price_per_unit, total_sales, operating_profit, operating_margin,
  avg_unit_price, profit_per_unit, is_high_margin,
  source_system, source_entity
)
SELECT
  s.source_id,                       -- just the CSV number
  s.invoice_ts,
  COALESCE((
    SELECT cs.customer_id
    FROM bl_3nf.ce_customer_scd cs
    WHERE cs.customer_src_id = COALESCE(s.customer_id,0)
      AND cs.is_active = TRUE
    LIMIT 1
  ), -1),
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
  s.source_system, s.source_entity
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
ON CONFLICT ON CONSTRAINT uq_ce_sales_nk DO NOTHING;




INSERT INTO bl_3nf.ce_sales (
  sale_src_id, invoice_ts,
  customer_id, product_id, retailer_id, city_id, sales_method_id, payment_method_id,
  units_sold, price_per_unit, total_sales, operating_profit, operating_margin,
  avg_unit_price, profit_per_unit, is_high_margin,
  source_system, source_entity
)
SELECT
  s.source_id,                       -- just the CSV number
  s.invoice_ts,
  COALESCE((
    SELECT cs.customer_id
    FROM bl_3nf.ce_customer_scd cs
    WHERE cs.customer_src_id = COALESCE(s.customer_id,0)
      AND cs.is_active = TRUE
    LIMIT 1
  ), -1),
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
  s.source_system, s.source_entity
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
ON CONFLICT ON CONSTRAINT uq_ce_sales_nk DO NOTHING;









-- INSERT INTO bl_3nf.ce_sales (
--   sale_src_id, invoice_ts,
--   customer_id, product_id, retailer_id, city_id, sales_method_id, payment_method_id,
--   units_sold, price_per_unit, total_sales, operating_profit, operating_margin,
--   avg_unit_price, profit_per_unit, is_high_margin,
--   source_system, source_entity
-- )
-- SELECT
--   s.source_id,                       -- just the CSV number
--   s.invoice_ts,
--   COALESCE((
--     SELECT cs.customer_id
--     FROM bl_3nf.ce_customer_scd cs
--     WHERE cs.customer_src_id = COALESCE(s.customer_id,0)
--       AND cs.is_active = TRUE
--     LIMIT 1
--   ), -1),
--   COALESCE(p.product_id,-1),
--   COALESCE(r.retailer_id,-1),
--   COALESCE(c.city_id,-1),
--   COALESCE(sm.sales_method_id,-1),
--   COALESCE(pm.payment_method_id,-1),
--   COALESCE(s.units_sold,0),
--   COALESCE(s.price_per_unit,0),
--   COALESCE(s.total_sales,0),
--   s.operating_profit,
--   s.operating_margin,
--   NULLIF(s.total_sales,0)::NUMERIC / NULLIF(s.units_sold,0)::NUMERIC,
--   CASE WHEN s.units_sold > 0 THEN (s.total_sales - COALESCE(s.operating_profit,0)) / s.units_sold END,
--   CASE WHEN s.operating_margin >= 0.30 THEN TRUE ELSE FALSE END,
--   s.source_system, s.source_entity
-- FROM sa_instore.src_instore_sales s
-- LEFT JOIN bl_3nf.ce_product p
--   ON p.source_system='SA_INSTORE' AND p.source_entity='SRC_INSTORE'
--  AND p.product_src_id = s.product_id
-- LEFT JOIN bl_3nf.ce_retailer r
--   ON r.source_system='SA_INSTORE' AND r.source_entity='SRC_INSTORE'
--  AND r.retailer_src_id = TRIM(COALESCE(s.retailer,'N/A'))
-- LEFT JOIN bl_3nf.ce_sales_method sm
--   ON sm.source_system='SA_INSTORE' AND sm.source_entity='SRC_INSTORE'
--  AND sm.sales_method_src_id = s.sales_method_id
-- LEFT JOIN bl_3nf.ce_payment_method pm
--   ON pm.source_system='SA_INSTORE' AND pm.source_entity='SRC_INSTORE'
--  AND pm.payment_method_src_id = s.payment_method_id
-- LEFT JOIN bl_3nf.ce_city c
--   ON c.city_id = s.city_id
-- ON CONFLICT ON CONSTRAINT uq_ce_sales_nk DO NOTHING;




-- INSERT INTO bl_3nf.ce_sales (
--   sale_src_id, invoice_ts,
--   customer_id, product_id, retailer_id, city_id, sales_method_id, payment_method_id,
--   units_sold, price_per_unit, total_sales, operating_profit, operating_margin,
--   avg_unit_price, profit_per_unit, is_high_margin,
--   source_system, source_entity
-- )
-- SELECT
--   s.source_id,                       -- just the CSV number
--   s.invoice_ts,
--   COALESCE((
--     SELECT cs.customer_id
--     FROM bl_3nf.ce_customer_scd cs
--     WHERE cs.customer_src_id = COALESCE(s.customer_id,0)
--       AND cs.is_active = TRUE
--     LIMIT 1
--   ), -1),
--   COALESCE(p.product_id,-1),
--   COALESCE(r.retailer_id,-1),
--   -1,
--   COALESCE(sm.sales_method_id,-1),
--   COALESCE(pm.payment_method_id,-1),
--   COALESCE(s.units_sold,0),
--   COALESCE(s.price_per_unit,0),
--   COALESCE(s.total_sales,0),
--   s.operating_profit,
--   s.operating_margin,
--   NULLIF(s.total_sales,0)::NUMERIC / NULLIF(s.units_sold,0)::NUMERIC,
--   CASE WHEN s.units_sold > 0 THEN (s.total_sales - COALESCE(s.operating_profit,0)) / s.units_sold END,
--   CASE WHEN s.operating_margin >= 0.30 THEN TRUE ELSE FALSE END,
--   s.source_system, s.source_entity
-- FROM sa_online.src_online_sales s
-- LEFT JOIN bl_3nf.ce_product p
--   ON p.source_system='SA_ONLINE' AND p.source_entity='SRC_ONLINE'
--  AND p.product_src_id = s.product_id
-- LEFT JOIN bl_3nf.ce_retailer r
--   ON r.source_system='SA_ONLINE' AND r.source_entity='SRC_ONLINE'
--  AND r.retailer_src_id = TRIM(COALESCE(s.retailer,'N/A'))
-- LEFT JOIN bl_3nf.ce_sales_method sm
--   ON sm.source_system='SA_ONLINE' AND sm.source_entity='SRC_ONLINE'
--  AND sm.sales_method_src_id = s.sales_method_id
-- LEFT JOIN bl_3nf.ce_payment_method pm
--   ON pm.source_system='SA_ONLINE' AND pm.source_entity='SRC_ONLINE'
--  AND pm.payment_method_src_id = s.payment_method_id
-- ON CONFLICT ON CONSTRAINT uq_ce_sales_nk DO NOTHING;