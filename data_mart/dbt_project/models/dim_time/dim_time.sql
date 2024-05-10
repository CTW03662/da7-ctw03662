WITH time_series AS (
    SELECT generate_series(
        '2023-01-01 00:00:00'::timestamp,  -- Start time
        '2023-01-01 23:59:59'::timestamp,  -- End time
        '1 second'::interval                -- Step interval
    ) as timestamp
)
SELECT
    timestamp::time as time,  -- Convert back to time, ignoring the date part
    Cast(EXTRACT(hour FROM timestamp) AS INTEGER) AS hour,
    Cast(EXTRACT(minute FROM timestamp) AS INTEGER) AS minutes,
    Cast(EXTRACT(second FROM timestamp) AS INTEGER) AS seconds
FROM time_series
