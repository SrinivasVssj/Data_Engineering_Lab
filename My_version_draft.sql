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