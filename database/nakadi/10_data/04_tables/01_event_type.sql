CREATE TABLE IF NOT EXISTS zn_data.event_type (
  et_name varchar(255) NOT NULL PRIMARY KEY CHECK (et_name <> ''),
  et_event_type_object jsonb NOT NULL,
  CHECK (et_event_type_object->>'name' = et_name)
);

CREATE INDEX ON zn_data.event_type USING gin (et_event_type_object);
