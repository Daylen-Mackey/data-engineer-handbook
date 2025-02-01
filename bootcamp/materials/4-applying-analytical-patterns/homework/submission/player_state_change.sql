-- Insert player state changes into players_state_tracking table
INSERT INTO players_state_tracking
WITH last_season AS (
    -- Select all records from the last season (1999)
    SELECT *
    FROM players_state_tracking
    WHERE season = 1999
),
current_season AS (
    -- Select player names and season for the current season (2000)
    SELECT
        player_name,
        season
    FROM player_seasons
    WHERE season = 2000
    GROUP BY player_name, season
)
SELECT
    -- Determine player name
    COALESCE(c.player_name, l.player_name) AS player_name,
    -- Determine first active season
    COALESCE(l.first_active_season, c.season) AS first_active_season,
    -- Determine last active season
    COALESCE(c.season, l.last_active_season) AS last_active_season,
    -- Determine player state
    CASE
        WHEN l.player_name IS NULL AND c.player_name IS NOT NULL THEN 'New'
        WHEN c.player_name IS NULL AND l.season = l.last_active_season THEN 'Retired'
        WHEN l.last_active_season < c.season - 1 THEN 'Returned from Retirement'
        WHEN l.last_active_season = c.season - 1 THEN 'Continued Playing'
        ELSE 'Stayed Retired'
    END::player_state AS player_state,  -- Cast to player_state type
    -- Determine seasons active
    COALESCE(l.seasons_active, ARRAY[]::INT[]) ||
        CASE WHEN c.player_name IS NOT NULL
        THEN ARRAY[c.season] ELSE ARRAY[]::INT[]
        END AS seasons_active,
    -- Determine season
    COALESCE(c.season, l.season + 1) AS season
FROM current_season c
FULL OUTER JOIN last_season l
    ON c.player_name = l.player_name;
