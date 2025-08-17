DROP TABLE IF EXISTS sa_instore.src_instore_sales;
CREATE TABLE sa_instore.src_instore_sales (
  source_system       VARCHAR(50)   NOT NULL,   -- 'SA_INSTORE'
  source_entity       VARCHAR(50)   NOT NULL,   -- 'EXT_INSTORE_SALES'
  source_id           VARCHAR(100)  NOT NULL,   -- from 'index'
  retailer            VARCHAR(255),
  retailer_id         BIGINT,
  region              VARCHAR(100),
  region_id           BIGINT,
  state               VARCHAR(100),
  state_id            BIGINT,
  city                VARCHAR(150),
  city_id             BIGINT,
  product             VARCHAR(255),
  product_id          BIGINT,
  product_category    VARCHAR(150),
  product_category_id BIGINT,
  price_per_unit      NUMERIC(12,2),
  units_sold          INT,
  total_sales         NUMERIC(14,2),
  operating_profit    NUMERIC(14,2),
  operating_margin    NUMERIC(7,4),       -- 0–1
  sales_method        VARCHAR(100),
  sales_method_id     INT,
  payment_method      VARCHAR(100),
  payment_method_id   INT,
  invoice_ts          TIMESTAMP,
  customer_id         BIGINT,
  customer_firstname  VARCHAR(100),
  customer_lastname   VARCHAR(100),
  insert_dt           TIMESTAMP DEFAULT NOW(),
  update_dt           TIMESTAMP DEFAULT NOW()
);

-- change text to varchar

INSERT INTO sa_instore.src_instore_sales (
  source_system, source_entity, source_id,
  retailer, retailer_id, region, region_id, state, state_id, city, city_id,
  product, product_id, product_category, product_category_id,
  price_per_unit, units_sold, total_sales, operating_profit, operating_margin,
  sales_method, sales_method_id, payment_method, payment_method_id,
  invoice_ts, customer_id, customer_firstname, customer_lastname
)
SELECT
  'SA_INSTORE' AS source_system,
  'EXT_INSTORE_SALES' AS source_entity,
  e.index AS source_id,
  e.retailer,
  NULLIF(REGEXP_REPLACE(e.retailer_id,        '[^0-9\-]', '', 'g'),'')::BIGINT AS retailer_id,
  e.region,
  NULLIF(REGEXP_REPLACE(e.region_id,          '[^0-9\-]', '', 'g'),'')::BIGINT AS region_id,
  e.state,
  NULLIF(REGEXP_REPLACE(e.state_id,           '[^0-9\-]', '', 'g'),'')::BIGINT AS state_id,
  e.city,
  NULLIF(REGEXP_REPLACE(e.city_id,            '[^0-9\-]', '', 'g'),'')::BIGINT AS city_id,
  e.product,
  NULLIF(REGEXP_REPLACE(e.product_id,         '[^0-9\-]', '', 'g'),'')::BIGINT AS product_id,
  e.product_category,
  NULLIF(REGEXP_REPLACE(e.product_category_id,'[^0-9\-]', '', 'g'),'')::BIGINT AS product_category_id,
  NULLIF(REGEXP_REPLACE(e.price_per_unit,     '[^0-9\.-]', '', 'g'),'')::NUMERIC(12,2) AS price_per_unit,
  NULLIF(REGEXP_REPLACE(e.units_sold,         '[^0-9\-]', '', 'g'),'')::INT              AS units_sold,
  NULLIF(REGEXP_REPLACE(e.total_sales,        '[^0-9\.-]', '', 'g'),'')::NUMERIC(14,2)  AS total_sales,
  NULLIF(REGEXP_REPLACE(e.operating_profit,   '[^0-9\.-]', '', 'g'),'')::NUMERIC(14,2)  AS operating_profit,
  (NULLIF(REGEXP_REPLACE(e.operating_margin,  '[^0-9\.-]', '', 'g'),'')::NUMERIC / 100.0)::NUMERIC(7,4) AS operating_margin,
  e.sales_method,
  NULLIF(REGEXP_REPLACE(e.sales_method_id,    '[^0-9\-]', '', 'g'),'')::INT              AS sales_method_id,
  e.payment_method,
  NULLIF(REGEXP_REPLACE(e.payment_method_id,  '[^0-9\-]', '', 'g'),'')::INT              AS payment_method_id,
  to_timestamp(e.invoice_date, 'FMMM/FMDD/YYYY HH24:MI') AS invoice_ts,
  NULLIF(REGEXP_REPLACE(e.customer_id,        '[^0-9\-]', '', 'g'),'')::BIGINT           AS customer_id,
  e.customer_firstname,
  e.customer_lastname
FROM sa_instore.ext_instore_sales e;



-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_schema = 'sa_instore'
--   AND table_name = 'src_instore_sales';

SELECT
    customer_id,
    product_id,
    invoice_ts,
    COUNT(*) AS dup_count
FROM sa_instore.src_instore_sales
GROUP BY customer_id, product_id, invoice_ts
HAVING COUNT(*) > 1;

DELETE FROM sa_instore.src_instore_sales a
USING (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, product_id, invoice_ts
            ORDER BY insert_dt
        ) AS rn
    FROM sa_instore.src_instore_sales
) b
WHERE a.ctid = b.ctid
  AND b.rn > 1;


SELECT * FROM sa_instore.src_instore_sales LIMIT 100;

















