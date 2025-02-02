-- - A query that uses window functions on `game_details` to find out the following things:
--   - What is the most games a team has won in a 90 game stretch?
--   - How many games in a row did LeBron James score over 10 points a game?


-- What is the most games a team has won in a 90 game stretch?
-- Columns to use

-- game_date_est
-- game_id
-- home_team_id
-- visitor_team_id
-- home_team_wins

-- Now we need to calculate the number of wins for each team in a 90 game stretch
-- We can use the window function to calculate the sum of wins for each team in a 90 game stretch
-- Calculate the number of wins for each team in a 90 game stretch using window functions

WITH team_wins AS (
    -- Combine home and visitor team wins into a single table
    SELECT
        game_date_est,
        game_id,
        home_team_id AS team_id,
        home_team_wins AS win
    FROM games
    UNION ALL
    SELECT
        game_date_est,
        game_id,
        visitor_team_id AS team_id,
        1 - home_team_wins AS win
    FROM games
),
win_streaks AS (
    -- Calculate the sum of wins for each team in a 90 game stretch
    SELECT
        team_id,
        game_date_est,
        game_id,
        SUM(win) OVER (
            PARTITION BY team_id
            ORDER BY game_date_est
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS win_count_90
    FROM team_wins
)
-- Find the maximum number of wins in a 90 game stretch for each team
SELECT
    team_id,
    MAX(win_count_90) AS max_wins_in_90_games
FROM win_streaks
GROUP BY team_id
ORDER BY max_wins_in_90_games DESC
LIMIT 1;



-- - How many games in a row did LeBron James score over 10 points a game?
WITH lbj_games AS (
    -- Extract LeBron’s games and ensure points are counted properly
    SELECT
        gd.player_name,
        gd.game_id,
        g.game_date_est,
        COALESCE(gd.pts, 0) AS scored_points
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James'
),

lbj_streaks AS (
    -- Assign a row number and a streak ID that resets when LeBron scores ≤10 points
    SELECT
        player_name,
        game_id,
        scored_points,
        game_date_est,
        -- Compute a streak ID that resets when LeBron scores ≤10 points
        SUM(CASE WHEN scored_points > 10 THEN 0 ELSE 1 END)
            OVER (PARTITION BY player_name ORDER BY game_date_est) AS streak_id
    FROM lbj_games
)

-- Find the longest streak in a single query reference
SELECT COUNT(*) AS longest_streak
FROM lbj_streaks
WHERE scored_points > 10
GROUP BY streak_id
order by longest_streak desc
limit 1
;
