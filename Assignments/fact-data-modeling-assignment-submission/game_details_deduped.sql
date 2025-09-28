-- Select * from public.game_details limit 5;
with game_deduped as (
select gd.*, row_number() over (PARTITION BY gd.* order by (select null))
 as rank_to_dedup from public.game_details gd
)

select * from game_deduped where rank_to_dedup = 1;