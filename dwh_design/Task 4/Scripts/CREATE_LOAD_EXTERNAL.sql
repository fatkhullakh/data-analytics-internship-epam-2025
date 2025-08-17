CREATE SCHEMA IF NOT EXISTS sa_instore;
CREATE SCHEMA IF NOT EXISTS sa_online;




CREATE EXTENSION IF NOT EXISTS file_fdw;

DROP SERVER IF EXISTS file_server CASCADE;
CREATE SERVER file_server FOREIGN DATA WRAPPER file_fdw;




DROP FOREIGN TABLE IF EXISTS sa_instore.ext_instore_sales;
CREATE FOREIGN TABLE sa_instore.ext_instore_sales (
  index               TEXT,
  retailer            TEXT,
  retailer_id         TEXT,
  region              TEXT,
  region_id           TEXT,
  state               TEXT,
  state_id            TEXT,
  city                TEXT,
  city_id             TEXT,
  product             TEXT,
  product_id          TEXT,
  product_category    TEXT,
  product_category_id TEXT,
  price_per_unit      TEXT,
  units_sold          TEXT,
  total_sales         TEXT,
  operating_profit    TEXT,
  operating_margin    TEXT,
  sales_method        TEXT,
  sales_method_id     TEXT,
  payment_method      TEXT,
  payment_method_id   TEXT,
  invoice_date        TEXT,   -- e.g. 2/17/2023 10:52
  customer_id         TEXT,
  customer_firstname  TEXT,
  customer_lastname   TEXT
)
SERVER file_server
OPTIONS (
  filename 'C:\EPAM\Adidas US Sales\Adidas US Sales\SA_INSTORE_SALES.csv',
  format 'csv',
  header 'true',
  delimiter ','
);




DROP FOREIGN TABLE IF EXISTS sa_online.ext_online_sales;
CREATE FOREIGN TABLE sa_online.ext_online_sales (
  index               TEXT,
  retailer            TEXT,
  retailer_id         TEXT,
  product             TEXT,
  product_id          TEXT,
  product_category    TEXT,
  product_category_id TEXT,
  price_per_unit      TEXT,
  units_sold          TEXT,
  total_sales         TEXT,
  operating_profit    TEXT,
  operating_margin    TEXT,
  sales_method        TEXT,
  sales_method_id     TEXT,
  payment_method      TEXT,
  payment_method_id   TEXT,
  invoice_date        TEXT,   -- e.g. 1/4/2023 10:07
  customer_id         TEXT,
  customer_firstname  TEXT,
  customer_lastname   TEXT
)
SERVER file_server
OPTIONS (
  filename 'C:\EPAM\Adidas US Sales\Adidas US Sales\SA_ONLINE_SALES.csv',
  format 'csv',
  header 'true',
  delimiter ','
);


SELECT * FROM sa_instore.ext_instore_sales LIMIT 10;
SELECT * FROM sa_online.ext_online_sales LIMIT 10;