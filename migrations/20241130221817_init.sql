CREATE TABLE event (
    id  TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    topic TEXT NOT NULL,
    aggregate TEXT NOT NULL,
    version INTEGER NOT NULL,
    data BLOB NOT NULL,
    metadata BLOB NULL,
    timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX idx_event_topic ON event(topic);
CREATE INDEX idx_event_topic_aggregate ON event(topic,aggregate);
CREATE UNIQUE INDEX idx_event_topic_aggregate_version ON event(topic,aggregate,version);

CREATE VIEW v_topic (
  name, 
  total_events
)
AS
  SELECT topic, COUNT(*)
  FROM event
  GROUP BY topic;
