CREATE TABLE IF NOT EXISTS default.events (
    event_id String,
    timestamp DateTime,
    event_type String,
    user_id UInt32,
    value Int32,
    metadata String,
    processed_at DateTime
) ENGINE = MergeTree()
ORDER BY (timestamp, event_id);

CREATE TABLE IF NOT EXISTS default.event_aggregates (
    window_start DateTime,
    window_end DateTime,
    event_type String,
    event_count UInt64,
    avg_value Float64
) ENGINE = SummingMergeTree()
ORDER BY (window_start, event_type);