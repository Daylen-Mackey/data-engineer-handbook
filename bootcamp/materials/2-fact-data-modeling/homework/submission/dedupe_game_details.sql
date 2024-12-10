-- Common Table Expression (CTE) to identify duplicates
WITH deduped AS (
    SELECT
        *,
        -- Assigns a row number to each row, grouped by game_id, team_id, and player_id
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS row_num
    FROM
        game_details
)

-- Select only the first occurrence of each duplicate group
SELECT
    *
FROM
    deduped
WHERE
    row_num = 1;
