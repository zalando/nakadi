#!/bin/bash

TEST_TOPIC_NAME="test-topic"

function wait_for() {
    while ! nc -z localhost $1 ; do sleep 1 ; done
}

echo Starting PostgreSQL
pg_ctlcluster ${PGVERSION} main start

echo "Creating database and user"
echo "CREATE ROLE nakadi WITH LOGIN CREATEROLE PASSWORD 'nakadi'; CREATE DATABASE local_nakadi_db OWNER nakadi;" \
    | sudo -u postgres psql -U postgres

touch /tmp/pg_ready

echo Starting ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null &
echo '################## Waiting for ZooKeeper to start'
wait_for 2181

echo Starting Kafka
bin/kafka-server-start.sh config/server.properties &
wait_for 9092

echo Creating topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic $TEST_TOPIC_NAME
echo '################## Topic Created'

wait

#while true; do
#    #bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic $TEST_TOPIC_NAME
#    #bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic $TEST_TOPIC_NAME
#    sleep 10
#done
