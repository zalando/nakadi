CREATE TABLE zn_data.storage
(
    st_id            VARCHAR(36) NOT NULL PRIMARY KEY,
    st_type          VARCHAR(32) NOT NULL,
    st_configuration JSONB       NOT NULL,
    st_default       BOOLEAN     NOT NULL DEFAULT FALSE
);
