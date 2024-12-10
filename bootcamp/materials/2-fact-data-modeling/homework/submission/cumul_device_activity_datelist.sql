-- - A cumulative query to generate `device_activity_datelist` from `events`
-- Insert cumulative device activity into user_devices_cumulated table
INSERT INTO user_devices_cumulated
WITH
-- Extract yesterday's data for the given date
yesterday AS (
    SELECT
        user_id,
        device_activity_datelist,
        browser_type,
        curr_date
    FROM user_devices_cumulated
    WHERE curr_date = DATE('2023-01-02') -- Filter for "yesterday's" date
),
-- Extract today's events and associated device information
today AS (
    SELECT DISTINCT
        event_time::DATE AS curr_date, -- Extract the date part from event_time
        events.user_id,
        devices.browser_type
    FROM events
    JOIN devices
        ON events.device_id = devices.device_id -- Join events with devices to get browser type
    WHERE events.user_id IS NOT NULL -- Ensure user_id is not null
      AND event_time::DATE = DATE('2023-01-03') -- Filter for "today's" date
)
-- Combine yesterday's and today's data into a cumulative format
SELECT
    COALESCE(yesterday.user_id, today.user_id) AS user_id, -- Use user_id from today if available, otherwise yesterday
    CASE
        WHEN yesterday.device_activity_datelist IS NULL THEN ARRAY[today.curr_date] -- If no history, initialize with today's date
        WHEN today.curr_date IS NULL THEN yesterday.device_activity_datelist -- If no event today, keep yesterday's history
        ELSE ARRAY[today.curr_date] || yesterday.device_activity_datelist -- Append today's date to yesterday's history
    END AS device_activity_datelist,
    COALESCE(yesterday.browser_type, today.browser_type) AS browser_type, -- Prefer today's browser type, fallback to yesterday's
    COALESCE(today.curr_date, yesterday.curr_date + INTERVAL '1 day') AS curr_date -- Use today's date or increment yesterday's by 1 day
FROM yesterday
FULL OUTER JOIN today
    ON yesterday.user_id = today.user_id -- Match by user_id
    AND yesterday.browser_type = today.browser_type; -- Match by browser type
