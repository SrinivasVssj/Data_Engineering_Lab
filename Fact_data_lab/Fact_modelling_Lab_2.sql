select * from public.events;
-- table to create cummulated dates active by user
DROP TABLE IF EXISTS public.user_cummulated;
create table public.user_cummulated
(
    user_id TEXT,
    dates_active date[], -- the list of dates the user was active
    current__date date, -- the current date of the last activity
    PRIMARY key (user_id,current__date)

);
-- building the pipeline and --trying to insert into user_cummulated

-- Insert into public.user_cummulated
-- with yesterday as (
--     select *
--     from public.user_cummulated
--     where current__date = DATE('2022-12-31')
-- ),
-- today as (
--     select cast(USER_ID as text) as user_id,
--            DATE(event_time) as date_active
--     from public.events
--     where DATE(event_time) = DATE('2023-01-01') and user_id is not NULL --null user ids happen because of local testing
--     group by user_id,date(event_time)
-- )


-- select  cast(COALESCE(t.user_id, y.user_id) as text) as user_id,
-- case when y.dates_active is  null and t.date_active is not null then array[t.date_active] 
--  when t.date_active is null then y.dates_active
--   else array[t.date_active] || y.dates_active  end as dates_active,
-- -- t.date_active,
-- case when t.date_active is not null then t.date_active else y.current__date + interval '1 day' end as current_date

-- from today t full outer join yesterday y on t.user_id = y.user_id;

SELECT * FROM public.user_cummulated;