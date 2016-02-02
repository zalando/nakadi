CREATE TABLE IF NOT EXISTS zsr_data.event_type (
  et_name varchar(255) NOT NULL PRIMARY KEY CHECK (et_name <> ''),
  et_event_type_object jsonb NOT NULL
);
