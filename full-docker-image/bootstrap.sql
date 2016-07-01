RESET ROLE;
CREATE ROLE nakadi_app WITH LOGIN PASSWORD 'nakadi';
GRANT zalando_nakadi_data_writer TO nakadi_app;
