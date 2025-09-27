
SET search_path = public, ctl, bl_cl, bl_dm, bl_3nf, sa_instore, sa_online;

CREATE SCHEMA IF NOT EXISTS ctl;

CREATE TABLE IF NOT EXISTS ctl.etl_log (
    log_id          BIGSERIAL PRIMARY KEY,
    log_ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    procedure_name  TEXT NOT NULL,
    rows_affected   BIGINT NOT NULL,
    message         TEXT
);

CREATE OR REPLACE PROCEDURE ctl.sp_log (
    p_proc  TEXT,
    p_rows  BIGINT,
    p_msg   TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ctl.etl_log (procedure_name, rows_affected, message)
    VALUES (p_proc, COALESCE(p_rows, 0), p_msg);
END
$$;

CREATE SCHEMA IF NOT EXISTS bl_cl;
CREATE SCHEMA IF NOT EXISTS bl_dm;

CREATE TABLE IF NOT EXISTS bl_dm.dim_date (
    date_key      INT PRIMARY KEY,
    full_date     DATE NOT NULL,
    year_num      INT,
    quarter_num   INT,
    month_num     INT,
    month_name    TEXT,
    day_of_month  INT,
    day_of_week   INT,
    day_name      TEXT,
    is_weekend    BOOLEAN
);

CREATE OR REPLACE FUNCTION bl_cl.fn_date_key (p_date DATE)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_key INT;
BEGIN
    IF p_date IS NULL THEN
        RETURN -1;
    END IF;

    v_key := (EXTRACT(YEAR FROM p_date)::INT * 10000)
           + (EXTRACT(MONTH FROM p_date)::INT * 100)
           +  EXTRACT(DAY FROM p_date)::INT;

    INSERT INTO bl_dm.dim_date (
        date_key, full_date, year_num, quarter_num, month_num, month_name,
        day_of_month, day_of_week, day_name, is_weekend
    )
    VALUES (
        v_key,
        p_date,
        EXTRACT(YEAR FROM p_date)::INT,
        EXTRACT(QUARTER FROM p_date)::INT,
        EXTRACT(MONTH FROM p_date)::INT,
        to_char(p_date, 'Mon'),
        EXTRACT(DAY FROM p_date)::INT,
        EXTRACT(ISODOW FROM p_date)::INT,
        to_char(p_date, 'Dy'),
        CASE WHEN EXTRACT(ISODOW FROM p_date) IN (6, 7) THEN TRUE ELSE FALSE END
    )
    ON CONFLICT (date_key) DO NOTHING;

    RETURN v_key;
END
$$;

CREATE OR REPLACE PROCEDURE bl_cl.dm_build_date (
    p_from DATE,
    p_to   DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO bl_dm.dim_date (
        date_key, full_date, year_num, quarter_num, month_num, month_name,
        day_of_month, day_of_week, day_name, is_weekend
    )
    SELECT
        (EXTRACT(YEAR FROM d)::INT * 10000
       +  EXTRACT(MONTH FROM d)::INT * 100
       +  EXTRACT(DAY FROM d)::INT)                     AS date_key,
        d                                               AS full_date,
        EXTRACT(YEAR FROM d)::INT                       AS year_num,
        EXTRACT(QUARTER FROM d)::INT                    AS quarter_num,
        EXTRACT(MONTH FROM d)::INT                      AS month_num,
        to_char(d, 'Mon')                               AS month_name,
        EXTRACT(DAY FROM d)::INT                        AS day_of_month,
        EXTRACT(ISODOW FROM d)::INT                     AS day_of_week,
        to_char(d, 'Dy')                                AS day_name,
        CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
    FROM generate_series(p_from, p_to, interval '1 day') AS g(d)
    ON CONFLICT (date_key) DO NOTHING;

    CALL ctl.sp_log('bl_cl.dm_build_date', 0, 'ok');
END
$$;

CREATE TABLE IF NOT EXISTS bl_dm.dim_product (
    product_key     BIGSERIAL PRIMARY KEY,
    product_src_id  BIGINT NOT NULL,
    product_name    TEXT   NOT NULL,
    category_src_id BIGINT NOT NULL,
    category_name   TEXT   NOT NULL,
    source_system   TEXT   NOT NULL,
    source_entity   TEXT   NOT NULL,
    CONSTRAINT uq_dm_product UNIQUE (source_system, source_entity, product_src_id)
);

CREATE TABLE IF NOT EXISTS bl_dm.dim_retailer (
    retailer_key     BIGSERIAL PRIMARY KEY,
    retailer_src_id  TEXT   NOT NULL,
    retailer_name    TEXT   NOT NULL,
    source_system    TEXT   NOT NULL,
    source_entity    TEXT   NOT NULL,
    CONSTRAINT uq_dm_retailer UNIQUE (source_system, source_entity, retailer_src_id)
);

CREATE TABLE IF NOT EXISTS bl_dm.dim_geography (
    geo_key     BIGSERIAL PRIMARY KEY,
    city_id     BIGINT NOT NULL,
    city_name   TEXT   NOT NULL,
    state_id    BIGINT NOT NULL,
    state_name  TEXT   NOT NULL,
    region_id   BIGINT NOT NULL,
    region_name TEXT   NOT NULL
);

DO $x$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_dm_geo_city'
          AND conrelid = 'bl_dm.dim_geography'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE bl_dm.dim_geography ADD CONSTRAINT uq_dm_geo_city UNIQUE (city_id)';
    END IF;
END
$x$;

CREATE TABLE IF NOT EXISTS bl_dm.dim_sales_method (
    sales_method_key     BIGSERIAL PRIMARY KEY,
    sales_method_src_id  INT  NOT NULL,
    sales_method_name    TEXT NOT NULL,
    source_system        TEXT NOT NULL,
    source_entity        TEXT NOT NULL,
    CONSTRAINT uq_dm_sales_method UNIQUE (source_system, source_entity, sales_method_src_id)
);

CREATE TABLE IF NOT EXISTS bl_dm.dim_payment_method (
    payment_method_key     BIGSERIAL PRIMARY KEY,
    payment_method_src_id  INT  NOT NULL,
    payment_method_name    TEXT NOT NULL,
    source_system          TEXT NOT NULL,
    source_entity          TEXT NOT NULL,
    CONSTRAINT uq_dm_payment_method UNIQUE (source_system, source_entity, payment_method_src_id)
);

CREATE TABLE IF NOT EXISTS bl_dm.dim_customer (
    customer_key     BIGSERIAL PRIMARY KEY,
    customer_src_id  BIGINT NOT NULL,
    first_name       TEXT,
    last_name        TEXT,
    full_name        TEXT,
    start_date_key   INT NOT NULL REFERENCES bl_dm.dim_date (date_key),
    end_date_key     INT NOT NULL REFERENCES bl_dm.dim_date (date_key),
    is_current       BOOLEAN NOT NULL,
    CONSTRAINT uq_dm_customer_version UNIQUE (customer_src_id, start_date_key, end_date_key)
);

CREATE TABLE IF NOT EXISTS bl_dm.fct_sales (
    date_key           INT    NOT NULL REFERENCES bl_dm.dim_date (date_key),
    product_key        BIGINT NOT NULL REFERENCES bl_dm.dim_product (product_key),
    retailer_key       BIGINT NOT NULL REFERENCES bl_dm.dim_retailer (retailer_key),
    geo_key            BIGINT NOT NULL REFERENCES bl_dm.dim_geography (geo_key),
    sales_method_key   BIGINT NOT NULL REFERENCES bl_dm.dim_sales_method (sales_method_key),
    payment_method_key BIGINT NOT NULL REFERENCES bl_dm.dim_payment_method (payment_method_key),
    customer_key       BIGINT NOT NULL REFERENCES bl_dm.dim_customer (customer_key),
    sale_src_id        TEXT   NOT NULL,
    source_system      TEXT   NOT NULL,
    source_entity      TEXT   NOT NULL,
    invoice_ts         TIMESTAMPTZ NOT NULL,
    units_sold         INT,
    price_per_unit     NUMERIC(12, 2),
    total_sales        NUMERIC(14, 2),
    operating_profit   NUMERIC(14, 2),
    operating_margin   NUMERIC(7, 4),
    avg_unit_price     NUMERIC(12, 4),
    profit_per_unit    NUMERIC(12, 4),
    is_high_margin     BOOLEAN,
    CONSTRAINT pk_fct_sales PRIMARY KEY (
        source_system, source_entity, sale_src_id, invoice_ts, product_key, retailer_key, customer_key
    )
) PARTITION BY RANGE (invoice_ts);

CREATE OR REPLACE PROCEDURE bl_cl.dm_refresh_fct_sales (
    p_from DATE,
    p_to   DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_month  DATE;
    v_start  TIMESTAMPTZ;
    v_end    TIMESTAMPTZ;
    v_part   TEXT;
    v_rows   BIGINT := 0;
BEGIN
    IF p_from IS NULL OR p_to IS NULL OR p_from > p_to THEN
        RAISE EXCEPTION 'Invalid window';
    END IF;

    v_month := date_trunc('month', p_from)::date;

    WHILE v_month <= p_to LOOP
        v_part  := format('bl_dm.fct_sales_%s', to_char(v_month, 'YYYYMM'));
        v_start := v_month;
        v_end   := (v_month + INTERVAL '1 month');

        IF NOT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'bl_dm'
              AND c.relname = format('fct_sales_%s', to_char(v_month, 'YYYYMM'))
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF bl_dm.fct_sales FOR VALUES FROM (%L) TO (%L)',
                v_part, v_start, v_end
            );
        END IF;

        EXECUTE format('TRUNCATE TABLE %I', v_part);

        v_month := (v_month + INTERVAL '1 month')::date;
    END LOOP;

    FOR v_part IN
        SELECT format('bl_dm.%s', c.relname)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'bl_dm'
          AND c.relname LIKE 'fct_sales_%'
    LOOP
        IF right(v_part, 6) ~ '^\d{6}$' THEN
            IF to_char(p_from, 'YYYYMM')::int > right(v_part, 6)::int THEN
                EXECUTE format('DROP TABLE IF EXISTS %s', v_part);
            END IF;
        END IF;
    END LOOP;

    v_month := date_trunc('month', p_from)::date;

    WHILE v_month <= p_to LOOP
        v_start := v_month;
        v_end   := (v_month + INTERVAL '1 month');

        WITH src AS (
            SELECT
                s.sale_src_id,
                s.invoice_ts,
                s.units_sold,
                s.price_per_unit,
                s.total_sales,
                s.operating_profit,
                s.operating_margin,
                s.avg_unit_price,
                s.profit_per_unit,
                s.is_high_margin,
                s.source_system,
                s.source_entity,
                dp.product_key,
                dr.retailer_key,
                dg.geo_key,
                dsm.sales_method_key,
                dpm.payment_method_key,
                bl_cl.fn_date_key(s.invoice_ts::date) AS date_key,
                s.customer_id
            FROM bl_3nf.ce_sales s
            JOIN bl_3nf.ce_product p
              ON p.product_id = s.product_id
            JOIN bl_dm.dim_product dp
              ON dp.source_system  = p.source_system
             AND dp.source_entity  = p.source_entity
             AND dp.product_src_id = p.product_id
            JOIN bl_3nf.ce_retailer r
              ON r.retailer_id = s.retailer_id
            JOIN bl_dm.dim_retailer dr
              ON dr.source_system   = r.source_system
             AND dr.source_entity   = r.source_entity
             AND dr.retailer_src_id = r.retailer
            LEFT JOIN bl_dm.dim_geography dg
              ON dg.city_id = s.city_id
            JOIN bl_3nf.ce_sales_method sm
              ON sm.sales_method_id = s.sales_method_id
            JOIN bl_dm.dim_sales_method dsm
              ON dsm.source_system       = sm.source_system
             AND dsm.source_entity       = sm.source_entity
             AND dsm.sales_method_src_id = sm.sales_method_id
            JOIN bl_3nf.ce_payment_method pm
              ON pm.payment_method_id = s.payment_method_id
            JOIN bl_dm.dim_payment_method dpm
              ON dpm.source_system          = pm.source_system
             AND dpm.source_entity          = pm.source_entity
             AND dpm.payment_method_src_id  = pm.payment_method_id
            WHERE s.invoice_ts >= v_start
              AND s.invoice_ts <  v_end
        ),
        cust AS (
            SELECT
                src.*,
                dc.customer_key
            FROM src
            JOIN bl_dm.dim_customer dc
              ON dc.customer_src_id = (
                   SELECT cs.customer_src_id
                   FROM bl_3nf.ce_customer_scd cs
                   WHERE cs.customer_id = src.customer_id
                   LIMIT 1
                 )
             AND src.date_key BETWEEN dc.start_date_key AND dc.end_date_key
        ),
        ins AS (
            INSERT INTO bl_dm.fct_sales (
                date_key,
                product_key,
                retailer_key,
                geo_key,
                sales_method_key,
                payment_method_key,
                customer_key,
                sale_src_id,
                source_system,
                source_entity,
                invoice_ts,
                units_sold,
                price_per_unit,
                total_sales,
                operating_profit,
                operating_margin,
                avg_unit_price,
                profit_per_unit,
                is_high_margin
            )
            SELECT
                cust.date_key,
                cust.product_key,
                cust.retailer_key,
                COALESCE(cust.geo_key, -1),
                cust.sales_method_key,
                cust.payment_method_key,
                cust.customer_key,
                cust.sale_src_id,
                cust.source_system,
                cust.source_entity,
                cust.invoice_ts,
                cust.units_sold,
                cust.price_per_unit,
                cust.total_sales,
                cust.operating_profit,
                cust.operating_margin,
                cust.avg_unit_price,
                cust.profit_per_unit,
                cust.is_high_margin
            FROM cust
            ON CONFLICT ON CONSTRAINT pk_fct_sales DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) INTO STRICT v_rows FROM ins;

        v_month := (v_month + INTERVAL '1 month')::date;
    END LOOP;

    CALL ctl.sp_log('bl_cl.dm_refresh_fct_sales', COALESCE(v_rows, 0), format('%s..%s', p_from, p_to));
END
$$;

DO $$
DECLARE
    v_min DATE;
    v_max DATE;
BEGIN
    SELECT MIN(invoice_ts)::date, MAX(invoice_ts)::date
    INTO v_min, v_max
    FROM bl_3nf.ce_sales;

    IF v_min IS NULL OR v_max IS NULL THEN
        v_min := DATE '2010-01-01';
        v_max := CURRENT_DATE;
    END IF;

    CALL bl_cl.dm_build_date(v_min, v_max);
END
$$;

DO $$
DECLARE
    v_to   DATE;
    v_from DATE;
BEGIN
    SELECT (date_trunc('month', MAX(invoice_ts)) + INTERVAL '1 month - 1 day')::date
    INTO v_to
    FROM bl_3nf.ce_sales;

    IF v_to IS NULL THEN
        v_to := CURRENT_DATE;
    END IF;

    v_from := (date_trunc('month', v_to)::date - INTERVAL '2 months')::date;

    CALL bl_cl.dm_refresh_fct_sales(v_from, v_to);
END
$$;

TABLE (
    SELECT *
    FROM ctl.etl_log
    WHERE procedure_name = 'bl_cl.dm_refresh_fct_sales'
    ORDER BY log_id DESC
    LIMIT 10
);

TABLE (
    SELECT to_char(invoice_ts, 'YYYY-MM') AS yyyymm, COUNT(*) AS rows
    FROM bl_dm.fct_sales
    GROUP BY 1
    ORDER BY 1 DESC
    LIMIT 6
);

TABLE (
    SELECT
        source_system,
        source_entity,
        sale_src_id,
        invoice_ts,
        product_key,
        retailer_key,
        customer_key,
        COUNT(*) AS dup_cnt
    FROM bl_dm.fct_sales
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    HAVING COUNT(*) > 1
);

TABLE (
    SELECT
        SUM(CASE WHEN product_key  IS NULL THEN 1 ELSE 0 END) AS null_product_key,
        SUM(CASE WHEN retailer_key IS NULL THEN 1 ELSE 0 END) AS null_retailer_key,
        SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key
    FROM bl_dm.fct_sales
);

WITH mx AS (
    SELECT date_trunc('month', MAX(invoice_ts)) AS m
    FROM bl_dm.fct_sales
)
TABLE (
    SELECT *
    FROM bl_dm.fct_sales, mx
    WHERE invoice_ts >= mx.m
      AND invoice_ts <  mx.m + INTERVAL '1 month'
    LIMIT 20
);
