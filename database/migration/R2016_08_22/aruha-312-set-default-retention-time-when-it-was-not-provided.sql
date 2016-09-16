UPDATE zn_data.event_type
SET et_event_type_object=jsonb_set(et_event_type_object, '{options}', '{"retention_time": 172800000}')
WHERE et_event_type_object->>'options' = '{"retention_time": null}' OR et_event_type_object->'options' is null;