CREATE SCHEMA if not exists bl_dm;

-- 2.1 Date dimension
CREATE TABLE bl_dm.dim_date (
  date_key     INT PRIMARY KEY,     -- yyyymmdd
  full_date    DATE NOT NULL,
  year_num     INT,
  quarter_num  INT,
  month_num    INT,
  month_name   TEXT,
  day_of_month INT,
  day_of_week  INT,
  day_name     TEXT,
  is_weekend   BOOLEAN
);

-- 2.2 Customer (SCD2 mirrored from 3NF)
CREATE TABLE bl_dm.dim_customer (
  customer_key     BIGSERIAL PRIMARY KEY,
  customer_src_id  BIGINT NOT NULL,
  first_name       TEXT,
  last_name        TEXT,
  full_name        TEXT,
  start_date_key   INT NOT NULL REFERENCES bl_dm.dim_date(date_key),
  end_date_key     INT NOT NULL REFERENCES bl_dm.dim_date(date_key),
  is_current       BOOLEAN NOT NULL,
  CONSTRAINT uq_dm_customer_version UNIQUE (customer_src_id, start_date_key, end_date_key)
);

-- 2.3 Product
CREATE TABLE bl_dm.dim_product (
  product_key      BIGSERIAL PRIMARY KEY,
  product_src_id   BIGINT  NOT NULL,
  product_name     TEXT    NOT NULL,
  category_src_id  BIGINT  NOT NULL,
  category_name    TEXT    NOT NULL,
  source_system    TEXT    NOT NULL,
  source_entity    TEXT    NOT NULL,
  CONSTRAINT uq_dm_product UNIQUE (source_system, source_entity, product_src_id)
);

-- 2.4 Retailer
CREATE TABLE bl_dm.dim_retailer (
  retailer_key     BIGSERIAL PRIMARY KEY,
  retailer_src_id  TEXT   NOT NULL,
  retailer_name    TEXT   NOT NULL,
  source_system    TEXT   NOT NULL,
  source_entity    TEXT   NOT NULL,
  CONSTRAINT uq_dm_retailer UNIQUE (source_system, source_entity, retailer_src_id)
);

-- 2.5 Geography (flattened city/state/region)
CREATE TABLE bl_dm.dim_geography (
  geo_key     BIGSERIAL PRIMARY KEY,
  city_id     BIGINT NOT NULL,
  city_name   TEXT   NOT NULL,
  state_id    BIGINT NOT NULL,
  state_name  TEXT   NOT NULL,
  region_id   BIGINT NOT NULL,
  region_name TEXT   NOT NULL
);
ALTER TABLE bl_dm.dim_geography
  ADD CONSTRAINT uq_dm_geo_city UNIQUE (city_id);   -- needed for ON CONFLICT

-- 2.6 Methods
CREATE TABLE bl_dm.dim_sales_method (
  sales_method_key     BIGSERIAL PRIMARY KEY,
  sales_method_src_id  INT    NOT NULL,
  sales_method_name    TEXT   NOT NULL,
  source_system        TEXT   NOT NULL,
  source_entity        TEXT   NOT NULL,
  CONSTRAINT uq_dm_sales_method UNIQUE (source_system, source_entity, sales_method_src_id)
);

CREATE TABLE bl_dm.dim_payment_method (
  payment_method_key     BIGSERIAL PRIMARY KEY,
  payment_method_src_id  INT    NOT NULL,
  payment_method_name    TEXT   NOT NULL,
  source_system          TEXT   NOT NULL,
  source_entity          TEXT   NOT NULL,
  CONSTRAINT uq_dm_payment_method UNIQUE (source_system, source_entity, payment_method_src_id)
);


-- ---------- 3) Unknown (-1) seed rows ----------
INSERT INTO bl_dm.dim_date(date_key, full_date, year_num, quarter_num, month_num, month_name,
                           day_of_month, day_of_week, day_name, is_weekend)
VALUES (-1, DATE '1900-01-01', 1900, 1, 1, 'Jan', 1, 1, 'Mon', FALSE)
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO bl_dm.dim_customer(customer_key, customer_src_id, first_name, last_name, full_name,
                               start_date_key, end_date_key, is_current)
SELECT -1, -1, 'Unknown','Unknown','Unknown', -1, -1, FALSE
WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_customer WHERE customer_key = -1);

INSERT INTO bl_dm.dim_product(product_key, product_src_id, product_name, category_src_id, category_name, source_system, source_entity)
SELECT -1, -1, 'N/A', -1, 'N/A', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_product WHERE product_key = -1);

INSERT INTO bl_dm.dim_retailer(retailer_key, retailer_src_id, retailer_name, source_system, source_entity)
SELECT -1, 'N/A', 'N/A', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_retailer WHERE retailer_key = -1);

INSERT INTO bl_dm.dim_geography(geo_key, city_id, city_name, state_id, state_name, region_id, region_name)
SELECT -1, -1, 'Unknown', -1, 'Unknown', -1, 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_geography WHERE geo_key = -1);

INSERT INTO bl_dm.dim_sales_method(sales_method_key, sales_method_src_id, sales_method_name, source_system, source_entity)
SELECT -1, -1, 'Unknown', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_sales_method WHERE sales_method_key = -1);

INSERT INTO bl_dm.dim_payment_method(payment_method_key, payment_method_src_id, payment_method_name, source_system, source_entity)
SELECT -1, -1, 'Unknown', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_payment_method WHERE payment_method_key = -1);

-- ---------- 4) Helper type + date utilities ----------
CREATE SCHEMA IF NOT EXISTS bl_cl;

CREATE OR REPLACE FUNCTION bl_cl.fn_date_key(p_date DATE)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE v_key INT;
BEGIN
  IF p_date IS NULL THEN RETURN -1; END IF;

  v_key := (EXTRACT(YEAR FROM p_date)::INT*10000)
        +  (EXTRACT(MONTH FROM p_date)::INT*100)
        +   EXTRACT(DAY FROM p_date)::INT;

  INSERT INTO bl_dm.dim_date(date_key, full_date, year_num, quarter_num, month_num, month_name,
                             day_of_month, day_of_week, day_name, is_weekend)
  VALUES (v_key, p_date,
          EXTRACT(YEAR FROM p_date)::INT,
          EXTRACT(QUARTER FROM p_date)::INT,
          EXTRACT(MONTH FROM p_date)::INT,
          to_char(p_date,'Mon'),
          EXTRACT(DAY FROM p_date)::INT,
          EXTRACT(ISODOW FROM p_date)::INT,
          to_char(p_date,'Dy'),
          CASE WHEN EXTRACT(ISODOW FROM p_date) IN (6,7) THEN TRUE ELSE FALSE END)
  ON CONFLICT (date_key) DO NOTHING;

  RETURN v_key;
END $$;

CREATE OR REPLACE PROCEDURE bl_cl.dm_build_date(p_from DATE, p_to DATE)
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO bl_dm.dim_date(date_key, full_date, year_num, quarter_num, month_num, month_name,
                             day_of_month, day_of_week, day_name, is_weekend)
  SELECT
    (EXTRACT(YEAR FROM d)::INT*10000 + EXTRACT(MONTH FROM d)::INT*100 + EXTRACT(DAY FROM d)::INT),
    d,
    EXTRACT(YEAR FROM d)::INT, EXTRACT(QUARTER FROM d)::INT, EXTRACT(MONTH FROM d)::INT,
    to_char(d,'Mon'), EXTRACT(DAY FROM d)::INT, EXTRACT(ISODOW FROM d)::INT, to_char(d,'Dy'),
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN TRUE ELSE FALSE END
  FROM generate_series(p_from, p_to, interval '1 day') g(d)
  ON CONFLICT (date_key) DO NOTHING;

  CALL ctl.sp_log('bl_cl.dm_build_date', 0, 'dim_date built/extended');
END $$;