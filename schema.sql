DROP TABLE IF EXISTS events_aggregation;

CREATE TABLE IF NOT EXISTS events_aggregation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    minute TEXT NOT NULL,
    event_count INTEGER
);

CREATE UNIQUE INDEX idx_customer_id_minute 
    ON events_aggregation (customer_id, minute);