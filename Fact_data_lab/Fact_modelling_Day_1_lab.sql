-- this file talks about fact modeeling day 1 lab
-- select game_id,team_id,player_id,

-- count(1) from game_details GROUP BY 1,2,3

-- HAVING COUNT(1) > 1
-- with game_deduped as (
-- select g.game_date_est,gd.*, row_number () over (partition by gd.game_id,
-- gd.team_id,gd.player_id order by g.game_date_est) as row_num
-- from public.game_details gd join public.games g
-- on g.game_id = gd.game_id
-- )

-- select * from game_deduped where row_num = 1 
-- limit 100000;

--The "when" factor of every fact table is actually
--missing from current table, we will get it from 
--public.games, also, the above table is denormalized -- means, it contains unnecessary columns which need to be taken care of
-- having game time is important because it shows a fact and number of games increase rapidly with time compared to team names so take out unnecessary columns and have only important columns
drop table if exists public.fact_game_details;
CREATE TABLE public.fact_game_details
(
    dim_game_date date ,
    dim_season integer ,
    dim_team_id integer ,
    dim_player_id integer ,
    dim_player_name text,
    dim_start_position text,
    dim_is_playing_at_home boolean ,
    dim_did_not_played boolean ,
    dim_did_not_dressed boolean ,
    dim_not_with_team boolean ,
    m_minutes real,
    m_fgm integer,
    m_fga integer,
    m_fg3m integer,
    m_fg3a integer,
    m_ftm integer,
    m_fta integer,
    m_oreb integer,
    m_dreb integer,
    m_reb integer,
    m_ast integer,
    m_stl integer,
    m_blk integer,
    m_turnovers integer,
    m_pf integer,
    m_pts integer,
    m_plus_minus integer,
    PRIMARY KEY(dim_game_date, dim_team_id, dim_player_id)
);
Insert into public.fact_game_details
with fact_table_prep as (

                with game_deduped as (
                                        select g.game_date_est,g.season,g.home_team_id,g.visitor_team_id,gd.*, row_number () over 
                                        (partition by gd.game_id,gd.team_id,gd.player_id order by g.game_date_est) as row_num
                                        from public.game_details gd join public.games g
                                        on g.game_id = gd.game_id --where g.game_date_est = '2016-10-01'
                                    )
-- select * from game_deduped where row_num = 1 ;

                select game_date_est as dim_game_date,
                season as dim_season,
                team_id as dim_team_id,
                player_id as dim_player_id,
                player_name as dim_player_name,
                start_position as dim_start_position,
                home_team_id = team_id   as dim_playing_at_home,


                coalesce(position('DNP' IN comment ),0)>0 AS dim_did_not_played , -- position finds arg 1 string  in arg 2
                coalesce(position('DND' IN comment ),0)>0 AS dim_did_not_dressed ,
                coalesce(position('NWT' IN comment ),0)>0 AS dim_not_with_team ,
                cast(split_part(min,':','1') as real)+cast(split_part(min,':','2') as real)/60 as m_minutes, --split_part splits string based on delimiter and gives part based on index
                    fgm as m_fgm,
                    fga as m_fga,
                    fg3m as m_fg3m,
                    fg3a as m_fg3a,
                    ftm as m_ftm,
                    fta as m_fta,
                    oreb as m_oreb,
                    dreb as m_dreb,
                    reb as m_reb,
                    ast as m_ast,
                    stl as m_stl,
                    blk as m_blk,
                    "TO" as m_turnovers,
                    pf as m_pf,
                    pts as m_pts,
                    plus_minus as m_plus_minus



                from game_deduped where row_num = 1
 )
-- the above query makes data, "fact table ready" and now we create and load fact table

select * from fact_table_prep;

-- the fact table is loaded
-- we can do few analytics on fact table like number of games a player missed and related analysis
with player_miss_analysis as (
select dim_player_name as player_name,count(1) as num_games,count( case when dim_did_not_played then 1 end) as num_games_missed
,sum(m_pts) as total_points
from public.fact_game_details
group by dim_player_name )
select *, cast (num_games_missed / num_games as real) as pct_games_missed from player_miss_analysis order by pct_games_missed desc;
