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

SET ROLE TO zalando_schemaregistry_data_owner;
