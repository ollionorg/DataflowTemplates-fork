CREATE TABLE IF NOT EXISTS singers (
    singerid int PRIMARY KEY,
    firstname text,
    lastname text,
    shardid text,
    update_ts timestamp,
);