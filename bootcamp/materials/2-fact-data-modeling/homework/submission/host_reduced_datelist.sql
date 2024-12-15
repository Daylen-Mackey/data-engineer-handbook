-- Step 1: Aggregate daily data from the events table for the current day
WITH daily_activity AS (
    SELECT
        event_time::DATE AS activity_date,            -- The current activity date (truncated to day)
        host,                                         -- The host identifier
        CAST(COUNT(*) AS INT) AS daily_hits,          -- Total daily hits for the host
        CAST(COUNT(DISTINCT user_id) AS INT) AS daily_unique_users -- Total daily unique users for the host
    FROM events
    WHERE event_time::DATE = DATE('2023-01-03')       -- Filter for today's activity
    GROUP BY 1, 2                                    -- Group by activity date and host
),

-- Step 2: Retrieve cumulative data from the host_activity_reduced table for the month
monthly_activity AS (
    SELECT
        date_month,                                   -- The starting date of the month
        host,                                        -- The host identifier
        hit_array,                                   -- Cumulative array of daily hits for the month
        unique_visitors                              -- Cumulative array of daily unique visitors for the month
    FROM host_activity_reduced
    WHERE date_month = DATE('2023-01-01')            -- Filter for the specific month
)

-- Step 3: Insert or update the cumulative table with the new daily data
INSERT INTO host_activity_reduced (date_month, host, hit_array, unique_visitors)
SELECT
    -- Use the month's starting date; prioritize activity date if available, otherwise use the existing month start
    COALESCE(DATE_TRUNC('month', da.activity_date), ma.date_month) AS date_month,
    COALESCE(da.host, ma.host) AS host,             -- Use the host from daily activity if available, otherwise from monthly data
    -- Update the hit array: append today's hits to the existing array or initialize a new one with padding
    CASE
        WHEN ma.hit_array IS NOT NULL
            THEN ma.hit_array || ARRAY[COALESCE(da.daily_hits, 0)]  -- Append today's hits to the cumulative array
        ELSE
            ARRAY_FILL(0, ARRAY[COALESCE(GREATEST(da.activity_date - ma.date_month, 0), 1)])
            || ARRAY[COALESCE(da.daily_hits, 0)]                   -- Initialize a new array with padding
    END AS hit_array,
    -- Update the unique visitors array similarly
    CASE
        WHEN ma.unique_visitors IS NOT NULL
            THEN ma.unique_visitors || ARRAY[COALESCE(da.daily_unique_users, 0)] -- Append today's unique users
        ELSE
            ARRAY_FILL(0, ARRAY[COALESCE(GREATEST(da.activity_date - ma.date_month, 0), 1)])
            || ARRAY[COALESCE(da.daily_unique_users, 0)] -- Initialize a new array with padding
    END AS unique_visitors
FROM daily_activity da
FULL OUTER JOIN monthly_activity ma
    ON da.host = ma.host                             -- Match records by host

-- Handle conflicts on the (date_month, host) unique constraint
ON CONFLICT (date_month, host) DO UPDATE
SET
    hit_array = EXCLUDED.hit_array,                  -- Update hit_array with the new cumulative array
    unique_visitors = EXCLUDED.unique_visitors;      -- Update unique_visitors with the new cumulative array
