CREATE TYPE az_role AS ENUM('admin', 'readers', 'writers');

CREATE TABLE zn_data.authorization (
  az_id              UUID                     NOT NULL PRIMARY KEY,
  az_resource        VARCHAR(255)             NOT NULL,
  az_list            az_role                  NOT NULL,
  az_data_type       VARCHAR(255)             NOT NULL,
  az_value           VARCHAR(255)             NOT NULL
);

CREATE INDEX ON zn_data.authorization (az_resource);