CREATE TYPE az_operation AS ENUM('ADMIN', 'READ', 'WRITE');

CREATE TABLE zn_data.authorization (
  az_resource        VARCHAR(255)             NOT NULL,
  az_operation       az_operation             NOT NULL,
  az_data_type       VARCHAR(255)             NOT NULL,
  az_value           VARCHAR(255)             NOT NULL,
  PRIMARY KEY (az_resource, az_operation, az_data_type, az_value)
);

CREATE INDEX ON zn_data.authorization (az_resource);
