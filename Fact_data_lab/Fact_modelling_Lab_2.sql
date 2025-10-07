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

--faster way to excetute for a range of dates

    DO $$
    DECLARE
        startdate DATE := DATE '2023-01-01';
        end_date   DATE := DATE '2023-01-31';
        currentdate DATE := DATE '2023-01-01';
    BEGIN
        WHILE currentdate <= end_date LOOP
            INSERT INTO public.user_cummulated
            WITH yesterday AS (
                SELECT *
                FROM public.user_cummulated
                WHERE current__date = (currentdate - INTERVAL '1 day')::date
            ),
            today AS (
                SELECT CAST(user_id AS TEXT) AS user_id,
                       DATE(event_time) AS date_active
                FROM public.events
                WHERE DATE(event_time) = DATE '2023-01-01' AND user_id IS NOT NULL
                GROUP BY user_id, DATE(event_time)
            )
            SELECT
                CAST(COALESCE(t.user_id, y.user_id) AS TEXT) AS user_id,
                CASE
                    WHEN y.dates_active IS NULL AND t.date_active IS NOT NULL THEN ARRAY[t.date_active]
                    WHEN t.date_active IS NULL THEN y.dates_active
                    ELSE ARRAY[t.date_active] || y.dates_active
                END AS dates_active,
                CASE
                    WHEN t.date_active IS NOT NULL THEN t.date_active
                    ELSE y.current__date + INTERVAL '1 day'
                END AS current__date
            FROM today t
            FULL OUTER JOIN yesterday y ON t.user_id = y.user_id;
            currentdate := currentdate + INTERVAL '1 day';
        END LOOP;
    END $$;

    --  SELECT * FROM public.user_cummulated order by current__date desc

--  truncate Table public.user_cummulated;

-- below we are trying to generate a bit value for each date in the series Eg: for last 5 days, if a specific user
-- was active on day 4 his bit value would be 1 at the position of 2 power of (total days - difference in days from current date
-- so for day 4 it would be 2 power of (5- (5-4)) = 2 power of 4 = 8 and in bit it would be 01000 and we get 
-- bit value for all previous days and do either total sum or bit_or operation to add all the bits together to get a final bit value
-- representing all the days the user was active in the last 5 days
with users as (select * from public.user_cummulated where current__date = date('2023-01-31')),
 series as (
    select generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day')::date as series_date
)
, bit_values_per_day as (
    select 
    case when dates_active @> array[series_date]
    then  cast(pow(2, 32 - (current__date - series_date)) as bigint )
    else 0 end   placeholder_int_value ,
    cast (case when dates_active @> array[series_date]
    then  cast(pow(2, 32 - (current__date - series_date)) as bigint )
    else 0 end  as BIT(32) ) as placeholder_bit_value,
     * from users u cross join series s 
    -- where USER_ID = '10060569187331700000'
)

select user_id, sum(placeholder_int_value) as total_int_value,
-- cast (cast(sum(placeholder_int_value) as bigint)as BIT(32)) as combined_int_bits,
bit_or(placeholder_bit_value) as total_bit_value ,
bit_count(bit_or(placeholder_bit_value)) as combined_bits_count,

bit_or(placeholder_bit_value) > B'0'::bit(32) as monthly_active,

(bit_or(placeholder_bit_value) & (B'11111110000000000000000000000000'::bit(32))) > B'0'::bit(32) as weekly_active,

(bit_or(placeholder_bit_value) & (B'10000000000000000000000000000000'::bit(32))) > B'0'::bit(32) as daily_active

from bit_values_per_day

group by user_id order by combined_bits_count desc;


--- the above is usually used in companies like meta, tiktok, etc to calculate daily active users, weekly active users, monthly active users
--- and also to calculate retention rates, churn rates, etc.

SELECT * from public.user_cummulated order by current__date desc;