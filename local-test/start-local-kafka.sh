#!/bin/bash

TEST_TOPIC_NAME="test-topic"

function wait_for() {
    while ! nc -z localhost $1 ; do sleep 1 ; done
}

echo Starting ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &
echo '################## Waiting for ZooKeeper to start'
wait_for 2181

echo Starting Kafka
bin/kafka-server-start.sh config/server.properties &
wait_for 9092

sleep 1

echo Creating topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic $TEST_TOPIC_NAME
echo '################## Topic Created'

#EID=0
#EVENT_TYPE="TestEvent"
#{ while true; do
#    bin/kafka-console-producer.sh --topic $TEST_TOPIC_NAME --broker-list localhost:9092 &> /dev/null << --EVENT--
#{ "event_type": "$EVENT_TYPE", "partitioning_key":  "$EID", "metadata": { "created", "$(date)", "eid": "$EID", "root_eid": "0", "scopes": [ "${EVENT_TYPE}.read", "${EVENT_TYPE}.write" ] } }
#--EVENT--
#    ((EID++))
#    sleep 1
#done } &

bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic $TEST_TOPIC_NAME

bash
wait
#echo ################ Start consuming...
#bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic $TEST_TOPIC_NAME 2> /dev/null
