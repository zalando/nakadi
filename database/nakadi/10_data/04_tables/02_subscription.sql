CREATE TABLE IF NOT EXISTS zn_data.subscription (
  s_id varchar(36) NOT NULL PRIMARY KEY CHECK (s_id <> ''),
  s_subscription_object jsonb NOT NULL,
  CHECK (s_subscription_object->>'id' = s_id),
  CHECK (s_subscription_object->>'owning_application' IS NOT NULL AND s_subscription_object->>'owning_application' <> ''),
  CHECK (s_subscription_object->>'event_types' IS NOT NULL AND s_subscription_object->>'event_types' <> '[]'),
  CHECK (s_subscription_object->>'use_case' IS NOT NULL AND s_subscription_object->>'use_case' <> '')
);

CREATE INDEX ON zn_data.subscription USING gin (s_subscription_object);

CREATE UNIQUE INDEX ON zn_data.subscription ((s_subscription_object->>'owning_application'),
                                             (s_subscription_object->>'event_types'),
                                             (s_subscription_object->>'use_case'));