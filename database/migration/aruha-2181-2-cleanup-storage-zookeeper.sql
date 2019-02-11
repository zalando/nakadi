-- Remove all obsolete nodes
update storage
set st_configuration = concat('{"zk_connection_string": "',st_configuration ->> 'zk_connection_string','"}')::jsonb;