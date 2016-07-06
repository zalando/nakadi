#!/bin/bash

find /docker-entrypoint-initdb.d -type f -name "*.sql" -exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -a -f '{}' \;