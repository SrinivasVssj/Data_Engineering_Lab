-- we are implementing reduced facts and shuffling concept int his lab
-- drop TABLE if EXISTS PUBLIC.array_metrics;
-- create Table if not EXISTS PUBLIC.array_metrics
-- (
--     user_id NUMERIC,
--     month_start DATE,
--     metric_name TEXT,
--     metric_array real[],
--      PRIMARY key (user_id, month_start, metric_name)
-- );

--  Building Reduced facts for for month of Jan 2023
--  we will loop through each day of the month and update or insert into array_metrics table
-- we will join daily aggregate with historical data of reduced facts until then to build the final table

DO $$
DECLARE
    c_d DATE := DATE '2023-01-01';
    end_date DATE := DATE '2023-01-31';
BEGIN
    WHILE c_d <= end_date LOOP
        INSERT INTO public.array_metrics
        WITH daily_agg AS (
            SELECT 
                user_id, 
                date(event_time) as current__date,
                count(1) as num_site_hits
            FROM PUBLIC.events
            WHERE DATE(event_time) = c_d AND USER_ID IS NOT NULL
            GROUP BY user_id, date(event_time)
        ),
        yesterday AS (
            SELECT * FROM PUBLIC.array_metrics
            WHERE month_start = DATE '2023-01-01'
        )
        SELECT
            coalesce(da.user_id, y.user_id) as user_id,
            COALESCE(y.month_start, date(date_trunc('month', da.current__date))) as MONTH_START,
            'site_hits' as metric_name,
            CASE 
                WHEN y.metric_array is not null THEN y.metric_array || array[coalesce(da.num_site_hits, 0)]
                WHEN y.metric_array is null THEN 
                    array_fill(0::real, ARRAY[cast(da.current__date - date(date_trunc('month',da.current__date)) as int)])
                    || array[coalesce(da.num_site_hits, 0)]
            END as metric_array
        FROM daily_agg da 
        FULL OUTER JOIN yesterday y ON da.user_id = y.user_id
        ON CONFLICT (user_id, month_start, metric_name) DO UPDATE
        SET metric_array = EXCLUDED.metric_array;

        c_d := c_d + INTERVAL '1 day';
    END LOOP;
END $$;

select cardinality(metric_array), count(1) -- cardinality gives number of elements in the array
from public.array_metrics
GROUP BY 1

-- some analysis queries
select * from public.array_metrics;

-- getting the total aggregrates for each day across all users for first 3 days
with three_day_agg as (
select metric_name, month_start, 
    array[sum(metric_array[1]),sum(metric_array[2]),sum(metric_array[3])] as total_metric from public.array_metrics

    GROUP BY metric_name, month_start

    )

    select metric_name, month_start,  total_daily_hits, 
    month_start + (cast(cast(nr-1 as text) || ' day' as interval) ) as daily_date from three_day_agg ta 
    CROSS join unnest(ta.total_metric) WITH ORDINALITY AS a(total_daily_hits,nr)

    select * from PUBLIC.array_metrics;