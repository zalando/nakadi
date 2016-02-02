CREATE ROLE zalando_nakadi_data_owner;
CREATE ROLE zalando_nakadi_data_reader;
CREATE ROLE zalando_nakadi_data_writer;

GRANT zalando_nakadi_data_reader TO zalando_nakadi_data_writer;
GRANT zalando_nakadi_data_owner TO nakadi;

ALTER DEFAULT PRIVILEGES FOR ROLE zalando_nakadi_data_owner GRANT SELECT ON SEQUENCES TO zalando_nakadi_data_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE zalando_nakadi_data_owner GRANT SELECT, REFERENCES ON TABLES TO zalando_nakadi_data_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE zalando_nakadi_data_owner GRANT USAGE ON SEQUENCES TO zalando_nakadi_data_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE zalando_nakadi_data_owner GRANT EXECUTE ON FUNCTIONS TO zalando_nakadi_data_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE zalando_nakadi_data_owner GRANT INSERT, DELETE, UPDATE ON TABLES TO zalando_nakadi_data_writer;

CREATE SCHEMA zn_data AUTHORIZATION zalando_nakadi_data_owner;
GRANT USAGE ON SCHEMA zn_data TO zalando_nakadi_data_reader;

SET ROLE TO zalando_nakadi_data_owner;
