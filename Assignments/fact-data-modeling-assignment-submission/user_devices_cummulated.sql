drop TABLE if EXISTS PUBLIC.user_devices_cumulated;
create Table if not EXISTS PUBLIC.user_devices_cumulated
(
    user_id NUMERIC,
    
    activity json ,
     PRIMARY key (user_id)
);


insert into public.user_devices_cumulated (user_id, activity)
    with devices_deduped as (
    select de.*, row_number() over (PARTITION BY de.* order by (select null))
    as rank_to_dedup from public.devices de 
    ),
    events_deduped as (
    select ev.*, row_number() over (PARTITION BY ev.user_id,ev.device_id,date(ev.event_time) order by (select null))
    as rank_to_dedup from public.events ev where user_id is not null

    ),
    eventsfinal_deduped as (
    select * from events_deduped where rank_to_dedup = 1
    ),
    devices_final_deduped as (
    select * from devices_deduped where rank_to_dedup = 1
    ),
    final_table as (
    select ev.user_id, de.browser_type,
    count(1) as num_events,ARRAY_AGG(DISTINCT date(ev.event_time)) as event_dates
    from eventsfinal_deduped ev join devices_final_deduped de on de.device_id = ev.device_id
    group by ev.user_id, de.browser_type )

    select user_id,json_object_agg(browser_type,event_dates) as activity
    from final_table  GROUP BY user_id 
    ON CONFLICT (user_id) DO UPDATE
    SET activity = EXCLUDED.activity;

 select * from public.user_devices_cumulated ;
