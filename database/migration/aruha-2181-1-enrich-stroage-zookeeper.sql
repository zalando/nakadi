-- this script should be rolled out before releasing 2181
-- Update exhibitor configs
update storage
SET st_configuration = jsonb_set(
    st_configuration,
    '{zk_connection_string}',
    concat('"exhibitor://', st_configuration ->> 'exhibitor_address', ':', st_configuration ->> 'exhibitor_port', st_configuration ->> 'zk_path', '"')::jsonb,
    true)
where st_configuration ->> 'zk_connection_string' is NULL and st_configuration ->> 'exhibitor_address' != '';

-- Update zookeeper configs
update storage
SET st_configuration = jsonb_set(
    st_configuration,
    '{zk_connection_string}',
    concat('"zookeeper://', st_configuration ->> 'zk_address', st_configuration ->> 'zk_path', '"')::jsonb,
    true)
where st_configuration ->> 'zk_connection_string' is NULL;

