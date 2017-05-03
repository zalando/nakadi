SET ROLE zalando_nakadi_data_owner;

ALTER TABLE zn_data.timeline
  ADD COLUMN tl_deleted BOOLEAN DEFAULT FALSE;