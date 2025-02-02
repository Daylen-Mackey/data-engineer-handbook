-- - A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
--   - Aggregate this dataset along the following dimensions
--     - player and team
--       - Answer questions like who scored the most points playing for one team?
--     - player and season
--       - Answer questions like who scored the most points in one season?
--     - team
--       - Answer questions like which team has won the most games?
-- Insert aggregated game details into game_details_aggregated table
INSERT INTO game_details_aggregated
WITH game_details_dedup AS (
    -- Deduplicate game details and ensure scored_points is not null
    SELECT
        game_id,
        team_id,
        player_name,
        COALESCE(pts, 0) AS scored_points
    FROM game_details
    GROUP BY game_id, team_id, player_name, scored_points
),
home_team_join AS (
    -- Join game details with games table for home team
    SELECT
        g.season,
        g.game_id,
        gd.team_id,
        g.home_team_wins AS team_wins,
        player_name,
        gd.scored_points
    FROM games g
    JOIN game_details_dedup gd
        ON g.game_id = gd.game_id
        AND g.home_team_id = gd.team_id
),
visitor_team_join AS (
    -- Join game details with games table for visitor team
    SELECT
        g.season,
        g.game_id,
        gd.team_id,
        1 - g.home_team_wins AS team_wins,
        player_name,
        gd.scored_points
    FROM games g
    JOIN game_details_dedup gd
        ON g.game_id = gd.game_id
        AND g.visitor_team_id = gd.team_id
),
union_teams AS (
    -- Union home and visitor team data
    SELECT * FROM home_team_join
    UNION ALL
    SELECT * FROM visitor_team_join
),
one_team_wins_for_all_players AS (
    -- Calculate team wins for all players
    SELECT
        season,
        game_id,
        team_id,
        player_name,
        CASE
            WHEN player_name = FIRST_VALUE(player_name) OVER (PARTITION BY season, game_id, team_id) THEN team_wins
            ELSE 0
        END AS team_wins,
        scored_points
    FROM union_teams
)

-- Aggregate data using GROUPING SETS
SELECT
    CASE
        WHEN GROUPING(season) = 1 AND GROUPING(player_name) = 1 AND GROUPING(team_id) = 0 THEN 'team_id'
        WHEN GROUPING(season) = 1 AND GROUPING(player_name) = 0 AND GROUPING(team_id) = 0 THEN 'team_id__player_name'
        WHEN GROUPING(season) = 0 AND GROUPING(player_name) = 0 AND GROUPING(team_id) = 1 THEN 'season__player_name'
    END AS aggregation_level,
    season,
    team_id,
    player_name,
    SUM(team_wins) AS match_wins,
    SUM(scored_points) AS total_points
FROM one_team_wins_for_all_players
GROUP BY GROUPING SETS (
    (team_id),
    (player_name, team_id),
    (season, player_name)
);

-- who scored the most points playing on a given team?
select *
from game_details_aggregated
where aggregation_level = 'team_id__player_name'
order by total_points desc
limit 15;

-- Who scored the most points each season
select *
from game_details_aggregated
where aggregation_level = 'season__player_name'
order by total_points desc
limit 15;

-- Which team won the most games overall?
select *
from game_details_aggregated
where aggregation_level = 'team_id'
order by wins desc
limit 15;
