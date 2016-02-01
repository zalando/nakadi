/*DO $$
    BEGIN
        PERFORM * FROM pg_roles WHERE rolname = 'zalando_schemaregistry_data_owner';
        IF FOUND THEN
            DROP OWNED BY zalando_schemaregistry_data_owner;
        END IF;
    END;
$$ language plpgsql;

DROP ROLE IF EXISTS schemaregistry_app;
DROP ROLE IF EXISTS zalando_schemaregistry_data_writer;
DROP ROLE IF EXISTS zalando_schemaregistry_data_reader;
DROP ROLE IF EXISTS zalando_schemaregistry_data_owner;

CREATE ROLE zalando_schemaregistry_data_owner;
CREATE ROLE zalando_schemaregistry_data_reader;
CREATE ROLE zalando_schemaregistry_data_writer;

GRANT zalando_schemaregistry_data_reader TO zalando_schemaregistry_data_writer;
GRANT zalando_schemaregistry_data_owner TO schemaregistry;

ALTER DEFAULT PRIVILEGES FOR ROLE zalando_schemaregistry_data_owner GRANT SELECT ON SEQUENCES TO zalando_schemaregistry_data_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE zalando_schemaregistry_data_owner GRANT SELECT, REFERENCES ON TABLES TO zalando_schemaregistry_data_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE zalando_schemaregistry_data_owner GRANT USAGE ON SEQUENCES TO zalando_schemaregistry_data_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE zalando_schemaregistry_data_owner GRANT EXECUTE ON FUNCTIONS TO zalando_schemaregistry_data_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE zalando_schemaregistry_data_owner GRANT INSERT, DELETE, UPDATE ON TABLES TO zalando_schemaregistry_data_writer;

CREATE SCHEMA zsr_data AUTHORIZATION zalando_schemaregistry_data_owner;
GRANT USAGE ON SCHEMA zsr_data TO zalando_schemaregistry_data_reader;

CREATE ROLE schemaregistry_app with login password 'schemaregistry';
GRANT zalando_schemaregistry_data_writer TO schemaregistry_app;

SET ROLE TO zalando_schemaregistry_data_owner;
*/

DROP SCHEMA IF EXISTS zsr_data CASCADE;
CREATE SCHEMA zsr_data;
CREATE TABLE IF NOT EXISTS zsr_data.event_type (
  et_name varchar(255) NOT NULL primary key CHECK (et_name <> ''),
  et_event_type_object jsonb NOT NULL
);