SET ROLE zalando_nakadi_data_owner;

CREATE INDEX CONCURRENTLY subscription_owning_app_idx ON zn_data.subscription ((s_subscription_object ->> 'owning_application'));
