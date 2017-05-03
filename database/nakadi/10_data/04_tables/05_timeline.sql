CREATE TABLE zn_data.timeline (
  tl_id              UUID                     NOT NULL PRIMARY KEY,
  et_name            VARCHAR(255)             NOT NULL REFERENCES zn_data.event_type (et_name),
  tl_order           INT                      NOT NULL,
  st_id              VARCHAR(36)              NOT NULL REFERENCES zn_data.storage (st_id),
  tl_topic           VARCHAR(255)             NOT NULL,
  tl_created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
  tl_switched_at     TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  tl_cleanup_at      TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  tl_latest_position JSONB                    DEFAULT NULL,
  tl_deleted         BOOLEAN                  DEFAULT FALSE,
  UNIQUE (et_name, tl_order)
);

CREATE INDEX ON zn_data.timeline (et_name);
