drop Table if exists public.vertices cascade;
drop Table if exists public.edges cascade;

create type vertex_type AS ENUM (
   'player',
    'team',
    'game'
);

create type edge_type AS ENUM (
   'plays_against',
   'shares_team',
   'plays_in',
   'plays_on'
   );

CREATE TABLE IF NOT EXISTS public.vertices
(
   identifier TEXT,
   type vertex_type,
   properties JSON,
   PRIMARY KEY (identifier,type)
)
TABLESPACE pg_default;

create table If not exists public.edges
(
   subject_identifier TEXT,
   subject_type vertex_type,
   object_identifier TEXT,
   object_type vertex_type,
   edge_type edge_type,
    properties JSON,
    primary key(subject_identifier, subject_type, object_identifier, object_type, edge_type)
)

TABLESPACE pg_default;

INSERT into vertices
SELECT 
       game_id as identifier,
       'game'::vertex_type as type,
       json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', case when home_team_wins=1 then team_id_home else team_id_away end
       )       as properties
       
FROM public.games
WHERE game_id IS NOT NULL;

Insert into vertices
with players_agg as (
select player_id as identifier,
 max(player_name) as player_name,
--   'player'::vertex_type as type,
  count(1) as number_of_games,
  sum(pts) as total_points,
  array_agg(DISTINCT team_id) as teams
   from public.game_details GROUP BY player_id
)

-- SELECT * FROM public.game_details limit 10;

select identifier,
         'player'::vertex_type as type,
         
         json_build_object(
         'player_name', player_name,
         'number_of_games', number_of_games,
         'total_points', total_points,
         'teams', teams
         ) as properties
 from players_agg ;

Insert into vertices
with teams_deduped as (
select *, row_number () over (partition by team_id ) as row_num
from public.teams
)

 Select 
 team_id as identifier,
   'team'::vertex_type as type,
   json_build_object(
       'abbreviation', abbreviation,
       'nickname', nickname,
       'city', city,
       'arena', arena,
       'year_founded', yearfounded
   ) as properties
  from teams_deduped
 where row_num = 1;

insert into edges
with edges_deduped as (
select *, row_number () over (partition by player_id,game_id ) as row_num
FROM public.game_details
) 
 Select 
 player_id as subject_identifier,
  'player'::vertex_type as subject_type,
   game_id as object_identifier,
  'game'::vertex_type as object_type,
  'plays_in'::edge_type as edge_type,
   json_build_object(
      'start_position', start_position,
      'pts', pts,
      'team_id', team_id,
      'team_abbreviation', team_abbreviation
   ) as properties
    from edges_deduped where row_num = 1;

-- select v.*,e.* from edges e join vertices v on e.subject_type = v.type and e.subject_identifier = v.identifier;


-- select v.properties->>'player_name' as player_name,max(cast(e.properties ->> 'pts' as integer)) as max_points_in_a_game from edges e join vertices v on e.subject_type = v.type 
-- and e.subject_identifier = v.identifier group by 1 order by 2 desc;


Insert into edges
with edges_deduped as
(
   select *, row_number () over (partition by player_id,game_id ) as row_num
   FROM public.game_details
), 

filtered as ( select * from edges_deduped where row_num = 1) ,

edges_aggregated as 

(     select f1.player_id as subject_player_id,max(f1.player_name) as subject_player_name,
      f2.player_id as object_player_id,max(f2.player_name) as object_player_name,
      case when f1.team_abbreviation = f2.team_abbreviation then 'shares_team'::edge_type else 'plays_against'::edge_type 
      end  edge_type,
      count(1) as num_games, sum(f1.pts) as subject_points,
      sum(f2.pts) as object_points

      from filtered f1 join filtered f2 
      on f1.game_id = f2.game_id where f1.player_id <> f2.player_id and f1.player_id > f2.player_id 
      group by f1.player_id,f2.player_id,edge_type
)

select 
 subject_player_id as subject_identifier,
       'player'::vertex_type as subject_type,
       object_player_id as object_identifier,
       'player'::vertex_type as object_type,
       edge_type,
       json_build_object(
           
           'num_games', num_games,
           'subject_points', subject_points,
           'object_points', object_points
       ) as properties
 from edges_aggregated;

 select  from edges e join vertices v on e.subject_type = v.type and e.subject_identifier = v.identifier
  where e.subject_type = 'player'::vertex_type and e.edge_type = 'plays_against'::edge_type;
  --you can play with above query

--   SELECT pg_size_pretty(pg_total_relation_size('public.vertices')) AS vertices_size,
--          pg_size_pretty(pg_total_relation_size('public.edges')) AS edges_size;
--          above query will give you the size of the vertices and edges tables in a human-readable format.

select edge_type, count(*) from edges group by edge_type;