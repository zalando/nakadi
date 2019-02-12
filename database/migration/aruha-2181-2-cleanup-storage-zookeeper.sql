-- this script should be used after 2181 is successfully released
-- Remove all obsolete nodes
update storage
set st_configuration = concat('{"zk_connection_string": "',st_configuration ->> 'zk_connection_string','"}')::jsonb;