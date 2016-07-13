ALTER TABLE zn_data.event_type ADD COLUMN et_topic varchar(255) NOT NULL DEFAULT '';

UPDATE zn_data.event_type
SET et_topic = subquery.et_name
FROM (SELECT et_name FROM zn_data.event_type) AS subquery
WHERE zn_data.event_type.et_name = subquery.et_name;

CREATE UNIQUE INDEX idx_et_topic ON zn_data.event_type (et_topic);

ALTER TABLE zn_data.event_type ALTER COLUMN et_topic DROP DEFAULT;