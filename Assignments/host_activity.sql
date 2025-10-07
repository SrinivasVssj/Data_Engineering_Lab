--  drop table if exists public.hosts_cumulated cascade; -- cascade will drop dependent objects as well
--  create table public.hosts_cumulated (
--      host TEXT,
--      current__date date,
--      dates_active date[] ,-- the list of dates the host was active
--       -- the current date of the last activity
--      PRIMARY key (host, current__date)
-- )
DO $$
    DECLARE
        startdate DATE := DATE '2023-01-01';
        end_date   DATE := DATE '2023-01-31';
        currentdate DATE := DATE '2023-01-01';
    BEGIN
        WHILE currentdate <= end_date LOOP
            insert into public.hosts_cumulated (host, current__date, dates_active)
            with
                events_deduped as (
                select ev.*, row_number() over (PARTITION BY ev.* order by (select null))
                as rank_to_dedup from public.events ev where user_id is not null

                ),
                events_final_deduped as (
                select *,date(event_time) as current__date from events_deduped where rank_to_dedup = 1
                ),
                -- select host, current__date from events_final_deduped group by host
                yesterday as (
                    SELECT * FROM public.hosts_cumulated
                    WHERE current__date = (currentdate - INTERVAL '1 day')::date
                ),
                daily_agg as (
            
                SELECT 
                            host, 
                            current__date
                            -- array_agg(date('2022-12-12')) as dates_active -- placeholder for now
                            -- count(1) as num_site_hits
                        FROM events_final_deduped
                        WHERE current__date = currentdate AND host IS NOT NULL
                        GROUP BY host, current__date
                )
                select coalesce(da.host,y.host) as host,
                case when da.current__date is not null then da.current__date else y.current__date end as current__date,
                case when da.current__date is null and y.dates_active is not null then y.dates_active
                    when da.current__date is not null and y.dates_active is null then array[da.current__date] 
                    when da.current__date is not null and y.dates_active is not null then array[da.current__date] || y.dates_active
                    else y.dates_active end
                as dates_active from daily_agg da full outer join yesterday y on da.host = y.host

            on conflict (host,current__date) do update
            set dates_active = EXCLUDED.dates_active;
        
            currentdate := currentdate + INTERVAL '1 day';
        END LOOP;
END $$;

-- --     -- select host,array_agg(event_time ORDER BY event_time asc) as activity_list from final_table group by host;
-- -- truncate table public.hosts_cumulated

select * from public.hosts_cumulated;