select
    cast(d as date) as "date",
    cast(extract(day from d) as int) as "day",
    cast(extract(month from d) as int) as "month",
    TO_CHAR(d, 'Month') AS month_description,
    cast(EXTRACT(YEAR FROM d) as int) AS year,
    TO_CHAR(d, 'Day') AS week_day,
    cast(EXTRACT(WEEK FROM d) as int) AS week_number,
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM d) IN (9, 10, 11) THEN 'Autumn'
        ELSE 'Winter'
    END AS season
FROM
    generate_series('2023-01-01'::date, '2024-01-01'::date, '1 day'::interval) as d