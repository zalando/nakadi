SET ROLE zalando_nakadi_data_owner;

ALTER TABLE zn_data.storage
    ADD COLUMN st_default BOOLEAN NOT NULL DEFAULT false;