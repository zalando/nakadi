find /docker-entrypoint-initdb.d -type f -name "*.sql" | sort |
while read -r filename; do psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -a -f "$filename"; done