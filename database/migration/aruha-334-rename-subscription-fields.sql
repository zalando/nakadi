UPDATE zn_data.subscription
SET s_subscription_object = replace(s_subscription_object::text, '"none"', '"default"')::jsonb
WHERE s_subscription_object::text LIKE '%"none"%';

UPDATE zn_data.subscription
SET s_subscription_object = replace(s_subscription_object::text, 'start_from', 'read_from')::jsonb
WHERE s_subscription_object::text LIKE '%start_from%';