CREATE TABLE IF NOT EXISTS event_type (
  id SERIAL PRIMARY KEY,
  name varchar(255) NOT NULL CHECK (name <> ''),
  data json
);