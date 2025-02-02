-- - A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
--   - Aggregate this dataset along the following dimensions
--     - player and team
--       - Answer questions like who scored the most points playing for one team?
--     - player and season
--       - Answer questions like who scored the most points in one season?
--     - team
--       - Answer questions like which team has won the most games?
DROP TABLE IF EXISTS game_details_aggregated;
CREATE TABLE IF NOT EXISTS game_details_aggregated (
    aggregation_level TEXT,
    season INT,
    team_id INT,
    player_name TEXT,
    wins INT,
    total_points INT
);
