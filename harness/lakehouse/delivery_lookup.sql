-- Run after a successful dbt build. Uses current Postgres dimension values.
-- ponytail: scans raw placements; promote to validated dbt order facts for production.
WITH placements AS (
    SELECT
        CAST(TRY_CAST(json_extract_scalar(body, '$.order_id') AS uuid) AS varchar) AS order_id,
        TRY_CAST(json_extract_scalar(body, '$.restaurant_id') AS integer) AS restaurant_id
    FROM (
        SELECT TRY(json_parse(from_utf8(payload))) AS body
        FROM lakehouse.bronze.raw_order_events
    )
    WHERE json_extract_scalar(body, '$.event_type') = 'order_placed'
),
order_restaurants AS (
    -- Collapse retries without multiplying deliveries; omit ambiguous/invalid mappings.
    SELECT order_id, min(restaurant_id) AS restaurant_id
    FROM placements
    WHERE order_id IS NOT NULL
    GROUP BY order_id
    HAVING count(DISTINCT restaurant_id) = 1
       AND count(*) = count(restaurant_id)
)
SELECT
    d.event_id,
    d.order_id,
    d.occurred_at,
    r.name AS restaurant_name,
    z.city,
    z.name AS zone_name
FROM lakehouse.analytics.stg_nomly__deliveries AS d
LEFT JOIN order_restaurants AS o ON d.order_id = o.order_id
LEFT JOIN postgres.public.restaurants AS r ON o.restaurant_id = r.restaurant_id
LEFT JOIN postgres.public.zones AS z ON r.zone_id = z.zone_id;
