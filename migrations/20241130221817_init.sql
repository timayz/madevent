CREATE TABLE event (
    id  TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    aggregate TEXT NOT NULL,
    version INTEGER NOT NULL,
    data BLOB NOT NULL,
    metadata BLOB NULL,
    topic TEXT NOT NULL,
    tenant TEXT NULL,
    timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX idx_event_topic ON event(topic);
CREATE INDEX idx_event_tenant_topic ON event(tenant,topic);
CREATE INDEX idx_event_topic_aggregate ON event(topic,aggregate);
CREATE UNIQUE INDEX idx_event_topic_aggregate_version ON event(topic,aggregate,version);

CREATE TABLE consumer (
    id  TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    cursor TEXT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NULL
);
