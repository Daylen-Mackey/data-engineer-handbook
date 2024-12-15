INSERT INTO hosts_cumulated
-- Step 1: Retrieve data for "yesterday" from the hosts_cumulated table
WITH yesterday AS (
    SELECT
        curr_date,                  -- The date for the host's cumulative activity
        host,                       -- The host identifier
        host_activity_datelist      -- The list of dates representing the host's activity
    FROM
        hosts_cumulated
    WHERE
        curr_date = DATE('2023-01-04') -- Filter for the specific date "yesterday"
),

-- Step 2: Retrieve data for "today" from the events table
today AS (
    SELECT
        MIN(event_time)::DATE AS curr_date, -- Select the earliest event_time for the host (aggregated for performance)
        host                                -- The host identifier
    FROM
        events
    WHERE
        event_time::DATE = DATE('2023-01-05') -- Filter for the specific date "today"
    GROUP BY
        host -- Ensure one record per host
)

-- Step 3: Combine data for "yesterday" and "today" using a FULL OUTER JOIN
SELECT
    COALESCE(yesterday.host, today.host) AS host, -- Use the host from "today" if "yesterday" is NULL, and vice versa
    CASE
        WHEN yesterday.host_activity_datelist IS NULL THEN ARRAY[today.curr_date] -- If no prior activity, initialize with today's date
        WHEN today.curr_date IS NULL THEN yesterday.host_activity_datelist       -- If no activity today, retain yesterday's history
        ELSE ARRAY[today.curr_date] || yesterday.host_activity_datelist          -- Append today's date to yesterday's activity
    END AS host_activity_datelist,
    COALESCE(today.curr_date, yesterday.curr_date + INTERVAL '1' day) AS curr_date -- Use today's date if available, or increment yesterday's date
FROM
    yesterday
FULL OUTER JOIN
    today
    ON yesterday.host = today.host; -- Match records by host to combine their activity
