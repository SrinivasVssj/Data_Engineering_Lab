
--  drop table if exists public.hosts_reduced_activity cascade; -- cascade will drop dependent objects as well
--  create table public.hosts_reduced_activity (
--      host TEXT,
--     month_start DATE,
--     metric_name TEXT,
--     user_array real[],
--    metric_array real[],
-- PRIMARY key (host, month_start, metric_name))
DO $$
DECLARE
    c_d DATE := DATE '2023-01-01';
    end_date DATE := DATE '2023-01-31';
BEGIN
    WHILE c_d <= end_date LOOP
        INSERT INTO public.hosts_reduced_activity
        with
            events_deduped as (
            select ev.*, row_number() over (PARTITION BY ev.* order by (select null))
            as rank_to_dedup from public.events ev where ev.user_id is not null and ev.device_id is not null and ev.host is not null

            ),
        
            events_final_deduped as (
            select *,date(event_time) as current__date from events_deduped where rank_to_dedup = 1
            ),
                        
            daily_agg as (

            select  evf.host as host,count(distinct evf.user_id) as user_array,count(1) as site_hits,
            evf.current__date as current__date from events_final_deduped evf
            -- full outer join devices_final_deduped df on evf.device_id = df.device_id 
            where evf.host is not null and evf.current__date = c_d
            group by evf.host,evf.current__date    ),

            yesterday AS (
                            SELECT * FROM PUBLIC.hosts_reduced_activity
                            WHERE month_start = DATE '2023-01-01'
                        )
            SELECT
                    coalesce(da.host, y.host) as host,
                    COALESCE(y.month_start, date(date_trunc('month', da.current__date))) as MONTH_START,
                    'site_hits' as metric_name,
                    CASE 
                        WHEN y.user_array is not null THEN y.user_array || array[coalesce(da.user_array, 0)]
                        WHEN y.user_array is null THEN 
                            array_fill(0::real, ARRAY[cast(da.current__date - date(date_trunc('month',da.current__date)) as int)])
                            || array[coalesce(da.user_array, 0)]
                    END as user_array,
                    CASE 
                        WHEN y.metric_array is not null THEN y.metric_array || array[coalesce(da.site_hits, 0)]
                        WHEN y.metric_array is null THEN 
                            array_fill(0::real, ARRAY[cast(da.current__date - date(date_trunc('month',da.current__date)) as int)])
                            || array[coalesce(da.site_hits, 0)]
                    END as metric_array
                FROM daily_agg da 
                FULL OUTER JOIN yesterday y ON da.host = y.host
                ON CONFLICT (host, month_start, metric_name) DO UPDATE
                SET metric_array = EXCLUDED.metric_array,user_array = EXCLUDED.user_array;
                c_d := c_d + INTERVAL '1 day';
    END LOOP;
END $$;


select * from public.hosts_reduced_activity;

select cardinality(metric_array),cardinality(user_array) from public.hosts_reduced_activity;
-- truncate table public.hosts_reduced_activity;