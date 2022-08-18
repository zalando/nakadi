CREATE SCHEMA IF NOT EXISTS zn_data;

-- Create az_operation type if it does not exist
CREATE OR REPLACE FUNCTION create_az_operation_type() RETURNS integer AS $$
DECLARE v_exists INTEGER;

BEGIN
    SELECT into v_exists (SELECT 1 FROM pg_type WHERE typname = 'zn_data.az_operation');
    IF v_exists IS NULL THEN
        CREATE TYPE zn_data.az_operation AS ENUM ('ADMIN', 'READ', 'WRITE');
    END IF;
    RETURN v_exists;
END;
$$ LANGUAGE plpgsql;

-- Call the function you just created
SELECT create_az_operation_type();

-- Remove the function you just created
DROP function create_az_operation_type();

CREATE TABLE IF NOT EXISTS zn_data.event_type (
  et_name varchar(255) NOT NULL PRIMARY KEY CHECK (et_name <> ''),
  et_event_type_object jsonb NOT NULL,
  CHECK (et_event_type_object->>'name' = et_name)
);

CREATE INDEX ON zn_data.event_type USING gin (et_event_type_object);

CREATE TABLE IF NOT EXISTS zn_data.event_type_schema (
  ets_event_type_name varchar(255) NOT NULL REFERENCES zn_data.event_type (et_name),
  ets_schema_object jsonb NOT NULL
);

CREATE INDEX ON zn_data.event_type_schema USING gin (ets_schema_object);

CREATE INDEX ON zn_data.event_type_schema ((ets_schema_object->>'version'));
CREATE INDEX ON zn_data.event_type_schema ((ets_schema_object->>'created_at'));

CREATE UNIQUE INDEX ON zn_data.event_type_schema ((ets_schema_object->>'version'),
                                                  (ets_event_type_name));
                                                 
CREATE TABLE IF NOT EXISTS zn_data.storage (
    st_id            VARCHAR(36) NOT NULL PRIMARY KEY,
    st_type          VARCHAR(32) NOT NULL,
    st_configuration JSONB       NOT NULL,
    st_default       BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS zn_data.timeline (
  tl_id              UUID                     NOT NULL PRIMARY KEY,
  et_name            VARCHAR(255)             NOT NULL REFERENCES zn_data.event_type (et_name),
  tl_order           INT                      NOT NULL,
  st_id              VARCHAR(36)              NOT NULL REFERENCES zn_data.storage (st_id),
  tl_topic           VARCHAR(255)             NOT NULL,
  tl_created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
  tl_switched_at     TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  tl_cleanup_at      TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  tl_latest_position JSONB                    DEFAULT NULL,
  tl_deleted         BOOLEAN                  DEFAULT FALSE,
  UNIQUE (et_name, tl_order)
);

CREATE INDEX ON zn_data.timeline (et_name);

CREATE TABLE IF NOT EXISTS zn_data.authorization (
  az_resource        VARCHAR(255)             NOT NULL,
  az_operation       zn_data.az_operation             NOT NULL,
  az_data_type       VARCHAR(255)             NOT NULL,
  az_value           VARCHAR(255)             NOT NULL,
  PRIMARY KEY (az_resource, az_operation, az_data_type, az_value)
);

CREATE INDEX ON zn_data.authorization (az_resource);

BEGIN;

CREATE TABLE IF NOT EXISTS zn_data.subscription (
  s_id varchar(36) NOT NULL PRIMARY KEY CHECK (s_id <> ''),
  s_subscription_object jsonb NOT NULL,
  s_key_fields_hash varchar(100) NOT NULL,
  CHECK ((s_subscription_object->>'id') = s_id),
  CHECK ((s_subscription_object->>'owning_application') IS NOT NULL AND (s_subscription_object->>'owning_application') <> ''),
  CHECK ((s_subscription_object->>'event_types') IS NOT NULL AND (s_subscription_object->>'event_types') <> '[]'),
  CHECK ((s_subscription_object->>'consumer_group') IS NOT NULL AND (s_subscription_object->>'consumer_group') <> ''),
  CONSTRAINT subscription_key_fields_hash_is_unique UNIQUE (s_key_fields_hash)
);

CREATE INDEX ON zn_data.subscription ((s_subscription_object->>'created_at'));

COMMIT;