UPDATE zn_data.subscription
SET s_subscription_object = jsonb_set(s_subscription_object, '{consumer_group}', '"default"')
WHERE s_subscription_object->>'consumer_group' = 'none';

UPDATE zn_data.subscription
SET s_subscription_object = replace(s_subscription_object::text, 'start_from', 'read_from')::jsonb
WHERE s_subscription_object::text LIKE '%start_from%';