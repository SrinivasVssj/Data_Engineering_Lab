select * from public.events;
-- table to create cummulated dates active by user
create table public.user_cummulated
(
    user_id bigint,
    dates_active date[], -- the list of dates the user was active
    current__date date, -- the current date of the last activity
    PRIMARY key (user_id,current__date)

);
-- building the pipeline
with yesterday as (
    select *
    from public.user_cummulated
    where current__date = DATE('2022-12-31')
),
today as (
    select *
    from public.events
    where DATE(event_time) = DATE('2023-01-01')
)