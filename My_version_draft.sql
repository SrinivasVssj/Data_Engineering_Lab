-- felt like my way of doing this, but need to work on this more , merge coulde be used here

-- Insert into public.user_cummulated
-- with 
-- yesterday as (
--     select *
--     from public.user_cummulated
--     where current__date <= DATE('2023-01-01')
-- ),
-- today as (
--     select cast(USER_ID as text) as user_id,
--            DATE(event_time) as date_active
--     from public.events
--     where DATE(event_time) = DATE('2023-01-02') and user_id is not NULL --null user ids happen because of local testing
--     group by user_id,date(event_time)
-- ),
-- final_set as (
--     select  cast(COALESCE(t.user_id, y.user_id) as text) as user_id,
--     case when y.dates_active is  null and t.date_active is not null then array[t.date_active] 
--      when t.date_active is null then y.dates_active
--       else array[t.date_active] || y.dates_active  end as dates_active,
--     -- t.date_active,
--     -- case when t.date_active is not null then t.date_active else y.current__date + interval '1 day' end as current_date
--    DATE('2023-01-02')  as current__date
--     from today t full outer join yesterday y on t.user_id = y.user_id
-- )

-- -- select * from final_set 
-- -- where user_id = '5804919602898318000';

--      SELECT user_id, count(1) FROM public.user_cummulated group by user_id 
--     --  where user_id = '5804919602898318000' ;
-- --5804919602898318000
-- -- TRUNCATE TABLE public.user_cummulated;



-- select s.series_date IN (SELECT unnest(dates_active)), * from users u cross join series s 
-- where USER_ID = '10060569187331700000';

-- -- select unnest(dates_active) from public.user_cummulated where user_id = '10060569187331700000' and current__date = date('2023-01-31');


---In the day 3 fact lab for reduced facts, I've tried something on my own before looking at the solution
-- joining daily aggregate for particular month and it's historical data and building pipeline to insert or update the array_metrics table
--97789717497840830 testing case

-- insert into public.array_metrics

with daily_agg as (
    select user_id, 
    date(event_time) as current__date,
    --  'num_site_hits' as metric_name,
    count(1) as num_site_hits
    from public.events
    WHERE DATE(event_time) = date('2023-01-02') and USER_ID is not null
    group by user_id,date(event_time)
),
yesterday as (
    select * from public.array_metrics
    where month_start = date('2023-01-01')
)


select
    coalesce(da.user_id, y.user_id) as user_id,
    COALESCE(y.month_start, date_trunc('month', da.current__date)) as current__date,
    'site_hits' as metric_name,
    CASE WHEN y.metric_array is not null  then y.metric_array || array[coalesce(da.num_site_hits, 0)]
    when y.metric_array is null then array_fill(0::real, ARRAY[cast(da.current__date - date('2023-01-01')  as int)])    || array[coalesce(da.num_site_hits, 0)]
     END as metric_array
     from daily_agg da full outer join yesterday y 
on da.user_id = y.user_id 
where da.user_id = 97789717497840830