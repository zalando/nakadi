CREATE TABLE IF NOT EXISTS event_type (
  et_name varchar(255) NOT NULL primary key CHECK (et_name <> ''),
  et_event_type_object jsonb NOT NULL
);
