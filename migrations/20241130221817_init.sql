CREATE TABLE event (
    id  TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    aggregate TEXT NOT NULL,
    version INTEGER NOT NULL,
    data BLOB NOT NULL,
    metadata BLOB NULL,
    timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX idx_event_aggregate ON event(aggregate);
CREATE UNIQUE INDEX idx_event_aggregate_version ON event(aggregate,version);
