DO $$
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
