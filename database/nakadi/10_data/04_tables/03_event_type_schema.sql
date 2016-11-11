CREATE TABLE IF NOT EXISTS zn_data.event_type_schema (
  ets_event_type_name varchar(255) NOT NULL REFERENCES zn_data.event_type (et_name),
  ets_schema_object jsonb NOT NULL
);

CREATE INDEX ON zn_data.event_type_schema USING gin (ets_schema_object);

CREATE INDEX ON zn_data.event_type_schema ((ets_schema_object->>'version'));
CREATE INDEX ON zn_data.event_type_schema ((ets_schema_object->>'created_at'));

CREATE UNIQUE INDEX ON zn_data.event_type_schema ((ets_schema_object->>'version'),
                                                  (ets_event_type_name));
