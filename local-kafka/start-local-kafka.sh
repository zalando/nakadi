#!/bin/bash

TEST_TOPIC_NAME="test-topic"

function wait_for() {
    while ! nc -z localhost $1 ; do sleep 1 ; done
}

echo Starting ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null &
echo '################## Waiting for ZooKeeper to start'
wait_for 2181


if [ "$USE_CONTAINER_IP" = "true" ]; then
    # this is a dirty hack to get the IP container runs on, we should invent something better
    KAFKA_HOST=$(awk 'NR==1 {print $1}' /etc/hosts)
fi
: ${KAFKA_HOST=localhost}

cat >> config/server.properties << --
advertised.host.name=${KAFKA_HOST}
advertised.host.port=9092
--

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
