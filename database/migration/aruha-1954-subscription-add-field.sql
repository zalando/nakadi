SET ROLE zalando_nakadi_data_owner;

UPDATE zn_data.subscription SET s_subscription_object = jsonb_set(s_subscription_object, '{updated_at}',
                                      concat('"', s_subscription_object ->>'created_at', '"')::jsonb, true);
