-- - A `datelist_int` generation query. Convert the `device_activity_datelist`
-- column into a `datelist_int` column
-- Step 1: Filter the data for a specific current date
WITH users AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE curr_date = DATE('2023-01-04') -- Select records for January 4, 2023
),

-- Step 2: Generate a series of dates based on the min and max dates in the table
series AS (
    SELECT generate_series(
        (SELECT MIN(curr_date) FROM user_devices_cumulated), -- Earliest date in the table
        (SELECT MAX(curr_date) FROM user_devices_cumulated), -- Latest date in the table
        '1 day'::interval -- Generate one date per day
    )::date AS date_series
),

-- Step 3: Cross join users and the generated date series, and calculate placeholder integers
bits AS (
    SELECT
        -- Calculate a placeholder integer for each date
        CASE
            WHEN device_activity_datelist @> ARRAY[series.date_series] -- Check if the date is in the activity list
            THEN
                CAST(POW(2, 32 - (curr_date - series.date_series)) AS BIGINT) -- Calculate bit position
            ELSE
                0 -- No activity on this date
        END AS temp_int,
        users.user_id, -- Include user ID for grouping
        series.date_series, -- Include the current date in the series
        users.curr_date -- Include the user's current date
    FROM
        users
    CROSS JOIN
        series -- Cross join users with the generated series of dates
)

-- Step 4: Aggregate the placeholder integers to compute the bitwise activity representation
SELECT
    user_id,
    -- Sum the placeholder integers and cast the result to a 32-bit binary representation
    CAST(SUM(temp_int) AS BIGINT)::BIT(32) AS device_activity
FROM
    bits
GROUP BY
    user_id; -- Group results by user ID
