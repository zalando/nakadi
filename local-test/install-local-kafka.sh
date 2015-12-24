#!/bin/bash

KAFKA_VERSION=${KAFKA_VERSION:-0.9.0.0}
SCALA_VERSION=${SCALA_VERSION:-2.11}
KAFKA_IMG=${KAFKA_IMG:-kafka_${SCALA_VERSION}-${KAFKA_VERSION}}

apt-get install --yes netcat

curl -O -s http://ftp.halifax.rwth-aachen.de/apache/kafka/${KAFKA_VERSION}/${KAFKA_IMG}.tgz
mkdir -p opt && echo "Created opt directory"
tar -xzf ${KAFKA_IMG}.tgz -C opt && rm ${KAFKA_IMG}.tgz && echo "Extracted $KAFKA_IMG"

cat >> opt/${KAFKA_IMG}/config/server.properties << --

advertised.host.name=${KAFKA_ADVERTISED_HOST_NAME:-localhost}
advertised.host.port=${KAFKA_ADVERTISED_HOST_PORT:-9092}
--
