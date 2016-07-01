#!/bin/bash

### Startup storages
cd $KAFKA_DIR
./start-local-storages.sh

# Wait for storage to start up
while ! bash -c '[ -f /tmp/pg_ready ]'; do sleep 1; done

cd /


### Bootstrap PostgreSQL
touch /full-bootstrap.sql

# read all files from the database directory (which comes from the development env) and execute those
for file in $(find /database-bootstrap/database/ -type f|sort); do
    cat ${file} >> /full-bootstrap.sql
done

cat /database-bootstrap/bootstrap.sql >> /full-bootstrap.sql

sudo -H -u postgres psql -f /full-bootstrap.sql local_nakadi_db


### Startup Nakadi
java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar nakadi.jar
