BEGIN;

SET ROLE zalando_nakadi_data_owner;

CREATE TABLE IF NOT EXISTS zn_data.subscription (
  s_id varchar(36) NOT NULL PRIMARY KEY CHECK (s_id <> ''),
  s_subscription_object jsonb NOT NULL,
  s_key_fields_hash varchar(100) NOT NULL,
  CHECK ((s_subscription_object->>'id') = s_id),
  CHECK ((s_subscription_object->>'owning_application') IS NOT NULL AND (s_subscription_object->>'owning_application') <> ''),
  CHECK ((s_subscription_object->>'event_types') IS NOT NULL AND (s_subscription_object->>'event_types') <> '[]'),
  CHECK ((s_subscription_object->>'consumer_group') IS NOT NULL AND (s_subscription_object->>'consumer_group') <> ''),
  CONSTRAINT subscription_key_fields_hash_is_unique UNIQUE (s_key_fields_hash)
);

CREATE INDEX ON zn_data.subscription ((s_subscription_object->>'created_at'));

COMMIT;