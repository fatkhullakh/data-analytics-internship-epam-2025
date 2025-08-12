DROP TABLE IF EXISTS dim_date;


-- Create the dim_date table
CREATE TABLE dim_date (
    date_surr_id   INT PRIMARY KEY,      -- YYYYMMDD
    date_dt        DATE NOT NULL,        -- actual date
    day_of_month   INT NOT NULL,         -- day number (1-31)
    month_name     TEXT NOT NULL,        -- e.g. 'January'
    month_no       INT NOT NULL,         -- month number (1-12)
    year_no        INT NOT NULL,         -- year (e.g. 2024)
    weekday_name   TEXT NOT NULL,        -- e.g. 'Monday'
    weekday_no     INT NOT NULL,         -- ISO weekday (1=Mon .. 7=Sun)
    is_weekend     BOOLEAN NOT NULL,     -- TRUE if Saturday or Sunday
    insert_dt      TIMESTAMP NOT NULL DEFAULT NOW(),
    update_dt      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Fill table with dates from 2020-01-01 to 2030-12-31
INSERT INTO dim_date (
    date_surr_id, date_dt, day_of_month, month_name, month_no, year_no,
    weekday_name, weekday_no, is_weekend
)
SELECT
    EXTRACT(YEAR FROM d)::INT * 10000 +
    EXTRACT(MONTH FROM d)::INT * 100 +
    EXTRACT(DAY FROM d)::INT              AS date_surr_id,
    d                                     AS date_dt,
    EXTRACT(DAY FROM d)::INT              AS day_of_month,
    TO_CHAR(d, 'Month')                   AS month_name,
    EXTRACT(MONTH FROM d)::INT            AS month_no,
    EXTRACT(YEAR FROM d)::INT             AS year_no,
    TO_CHAR(d, 'Day')                     AS weekday_name,
    EXTRACT(ISODOW FROM d)::INT           AS weekday_no,
    (EXTRACT(ISODOW FROM d) IN (6, 7))    AS is_weekend
FROM generate_series(
    DATE '2020-01-01',
    DATE '2030-12-31',
    INTERVAL '1 day'
) AS gs(d);

SELECT * FROM dim_date
