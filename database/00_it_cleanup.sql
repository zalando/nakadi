DO $$
    BEGIN
        PERFORM * FROM pg_roles WHERE rolname = 'zalando_nakadi_data_owner';
        IF FOUND THEN
            DROP OWNED BY zalando_nakadi_data_owner;
        END IF;
    END;
$$ language plpgsql;

DROP ROLE IF EXISTS nakadi_app;
DROP ROLE IF EXISTS zalando_nakadi_data_writer;
DROP ROLE IF EXISTS zalando_nakadi_data_reader;
DROP ROLE IF EXISTS zalando_nakadi_data_owner;
