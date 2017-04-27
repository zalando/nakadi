SET ROLE zalando_nakadi_data_owner;

DROP INDEX subscription_s_subscription_object_idx;
DROP INDEX subscription_expr_expr1_expr2_idx;

ALTER TABLE zn_data.subscription ADD COLUMN s_key_fields_hash varchar(100) NOT NULL DEFAULT '';

-- between these two alters we need to run Aruha771MigrationHelper script that fills "s_key_fields_hash" field for all existing subscriptions

ALTER TABLE zn_data.subscription ADD CONSTRAINT subscription_key_fields_hash_is_unique UNIQUE (s_key_fields_hash);