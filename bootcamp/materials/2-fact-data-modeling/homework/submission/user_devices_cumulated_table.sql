--- Question: A DDL for an `user_devices_cumulated` table that has:
--   - a `device_activity_datelist` which tracks a users active days by `browser_type`
--   - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
--     - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    -- device_activity_datelist MAP<STRING, ARRAY[DATE]>
    device_activity_datelist date[],
    browser_type text,
    curr_date date,
    primary key (curr_date, user_id, browser_type)
);
