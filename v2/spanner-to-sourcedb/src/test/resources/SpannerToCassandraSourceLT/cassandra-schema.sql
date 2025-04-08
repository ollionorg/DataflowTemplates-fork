CREATE TABLE IF NOT EXISTS singers (
    SingerId int PRIMARY KEY,
    FirstName text,
    LastName text,
    shardId text,
    update_ts timestamp,
);