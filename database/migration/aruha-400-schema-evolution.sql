CREATE TABLE IF NOT EXISTS zn_data.event_type_schema (
  ets_event_type_name varchar(255) NOT NULL REFERENCES zn_data.event_type (et_name),
  ets_schema_object jsonb NOT NULL
);

CREATE INDEX ON zn_data.event_type_schema USING gin (ets_schema_object);

CREATE INDEX ON zn_data.event_type_schema ((ets_schema_object->>'version'));
CREATE INDEX ON zn_data.event_type_schema ((ets_schema_object->>'created_at'));

CREATE UNIQUE INDEX ON zn_data.event_type_schema ((ets_schema_object->>'version'),
                                                  (ets_event_type_name));

UPDATE zn_data.event_type SET et_event_type_object  = jsonb_set(et_event_type_object, '{schema,version}', '"1.0.0"', true);

UPDATE zn_data.event_type
SET et_event_type_object  = jsonb_set(et_event_type_object, '{created_at}', '"2016-11-09T19:32:00Z"', true);

UPDATE zn_data.event_type
SET et_event_type_object  = jsonb_set(et_event_type_object, '{updated_at}', '"2016-11-09T19:32:00Z"', true);

UPDATE zn_data.event_type
SET et_event_type_object  = jsonb_set(et_event_type_object, '{schema,created_at}', '"2016-11-09T19:32:00Z"', true);

UPDATE zn_data.event_type
SET et_event_type_object  = jsonb_set(et_event_type_object, '{compatibility_mode}', '"forward"', true);

INSERT INTO zn_data.event_type_schema (ets_event_type_name, ets_schema_object)
SELECT et_name, et_event_type_object -> 'schema' FROM zn_data.event_type;
