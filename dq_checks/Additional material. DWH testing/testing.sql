SELECT
  (SELECT COUNT(*) FROM dwh_clients) AS dwh_clients,
  (SELECT COUNT(*) FROM s1_clients) + (SELECT COUNT(*) FROM s2_clients) AS sum_src;

SELECT
  (SELECT COUNT(*) FROM dm_main_dashboard) AS dm_rows,
  (SELECT COUNT(*) FROM dwh_sales)         AS dwh_sales_rows;



SELECT c.*
FROM dwh_clients c
JOIN s1_clients s1 ON c.client_src_id = s1.client_id
WHERE c.valid_from <> DATE '2000-01-01'
   OR c.valid_to   <> DATE '2100-01-01';



SELECT c.*
FROM dwh_clients c
JOIN s2_clients s2 ON c.client_src_id = s2.client_id
WHERE c.phone_number <> (COALESCE(s2.phone_code,'') || COALESCE(s2.phone_number,''));



SELECT c.*
FROM dwh_clients c
JOIN s2_clients s2 ON c.client_src_id = s2.client_id
WHERE c.middle_name <> 'N/A';



SELECT *
FROM dwh_clients
WHERE (valid_to >  DATE '2021-01-20' AND is_valid <> 'Y')
   OR (valid_to <= DATE '2021-01-20' AND is_valid <> 'N');





SELECT p.*, s1.cost
FROM dwh_products p
JOIN s1_products s1 ON p.product_src_id = s1.product_id
WHERE NULLIF(p.product_cost::text,'')::numeric(18,2) <> NULLIF(s1.cost,'')::numeric(18,2);



WITH s2_cost AS (
  SELECT DISTINCT product_id, product_price
  FROM s2_client_sales
)
SELECT p.*, s2.product_price
FROM dwh_products p
JOIN s2_cost s2 ON p.product_src_id = s2.product_id
WHERE NULLIF(p.product_cost::text,'')::numeric(18,2) <> NULLIF(s2.product_price,'')::numeric(18,2);




SELECT dl.*
FROM dwh_locations dl
JOIN s1_channels s1 ON dl.location_name = s1.channel_location
WHERE dl.location_src_id <> 'N/A';




SELECT dl.*, s2.location_id
FROM dwh_locations dl
JOIN s2_locations s2 ON dl.location_name = s2.locatiion_name
WHERE dl.location_src_id <> s2.location_id;



SELECT c.*
FROM dwh_channels c
LEFT JOIN dwh_locations l ON c.location_id = l.location_id
WHERE l.location_id IS NULL;



SELECT * FROM dwh_sales WHERE quantity IS NULL OR quantity < 0;


SELECT * FROM dwh_sales WHERE order_created IS NULL OR order_completed IS NULL;



SELECT d.*,
       (d.quantity * p.product_cost) AS expected_total
FROM dm_main_dashboard d
JOIN dwh_products p ON d.product_name = p.product_name
WHERE d.total_cost <> (d.quantity * p.product_cost);



SELECT client_id, COUNT(*) FROM dwh_clients GROUP BY client_id HAVING COUNT(*)>1;
SELECT sale_id,   COUNT(*) FROM dwh_sales   GROUP BY sale_id   HAVING COUNT(*)>1;


SELECT * FROM dwh_sales WHERE client_id IS NULL OR channel_id IS NULL OR product_id IS NULL;
