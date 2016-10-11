CREATE TABLE IF NOT EXISTS zn_data.event_type (
  et_name varchar(255) NOT NULL CHECK (et_name <> ''),
  et_topic varchar(255) NOT NULL,
  et_event_type_object jsonb NOT NULL,
  et_archived boolean DEFAULT FALSE,
  CHECK (et_event_type_object->>'name' = et_name)
);

CREATE INDEX ON zn_data.event_type USING gin (et_event_type_object);

CREATE UNIQUE INDEX idx_et_topic ON zn_data.event_type (et_topic);
