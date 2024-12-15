
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    curr_date DATE,
    PRIMARY KEY (host, curr_date)
);
