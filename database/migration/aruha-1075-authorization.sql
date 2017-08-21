CREATE TYPE az_operation AS ENUM('admin', 'readers', 'writers');

CREATE TABLE zn_data.authorization (
  az_id              UUID                     NOT NULL PRIMARY KEY,
  az_resource        VARCHAR(255)             NOT NULL,
  az_operation       az_operation             NOT NULL,
  az_data_type       VARCHAR(255)             NOT NULL,
  az_value           VARCHAR(255)             NOT NULL
);

CREATE INDEX ON zn_data.authorization (az_resource);
