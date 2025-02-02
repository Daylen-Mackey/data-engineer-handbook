-- - A query that does state change tracking for `players`
--   - A player entering the league should be `New`
--   - A player leaving the league should be `Retired`
--   - A player staying in the league should be `Continued Playing`
--   - A player that comes out of retirement should be `Returned from Retirement`
--   - A player that stays out of the league should be `Stayed Retired`


-- Need to use cumulative table design

CREATE TYPE player_state AS ENUM (
    'New',
    'Retired',
    'Continued Playing',
    'Returned from Retirement',
    'Stayed Retired'
);

drop table if exists players_state_tracking;
CREATE TABLE players_state_tracking (
     player_name TEXT,
     first_active_season INT,
     last_active_season INT,
     player_state player_state,
     seasons_active INT[],
     season INT,
     PRIMARY KEY (player_name, season)
 );
