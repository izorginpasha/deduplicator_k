CREATE TABLE IF NOT EXISTS events.events
(
    event_time DateTime,
    event_hash FixedString(32),
    payload String
)
ENGINE = MergeTree
ORDER BY (event_time, event_hash);
