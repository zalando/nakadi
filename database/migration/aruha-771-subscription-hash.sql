SET ROLE zalando_nakadi_data_owner;

-- 1) Create new column
ALTER TABLE zn_data.subscription ADD COLUMN s_key_fields_hash varchar(100) NOT NULL DEFAULT '';

-- 2) Release new version of Nakadi and switch all traffic

-- 3) Run script that fills hash for all subscriptions Aruha771MigrationHelper

-- 4) Create new constraint
ALTER TABLE zn_data.subscription ADD CONSTRAINT subscription_key_fields_hash_is_unique UNIQUE (s_key_fields_hash);

-- 5) Drop old constraints
DROP INDEX zn_data.subscription_s_subscription_object_idx;
DROP INDEX zn_data.subscription_expr_expr1_expr2_idx;