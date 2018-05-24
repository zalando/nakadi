SET ROLE zalando_nakadi_data_owner;
ALTER TABLE zn_data.event_type DROP COLUMN IF EXISTS et_topic;
DROP INDEX zn_data.idx_et_topic;
