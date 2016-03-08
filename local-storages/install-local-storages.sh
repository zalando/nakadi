#!/bin/bash

localedef -i ${LNG} -c -f ${ENCODING} -A /usr/share/locale/locale.alias ${LANG}

source /etc/lsb-release

echo "deb http://apt.postgresql.org/pub/repos/apt/ ${DISTRIB_CODENAME}-pgdg main" > /etc/apt/sources.list.d/pgdg.list
curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

apt-get update --yes
apt-get upgrade --yes
apt-get install --yes netcat sudo postgresql-${PGVERSION}

curl -O -s http://ftp.halifax.rwth-aachen.de/apache/kafka/${KAFKA_VERSION}/${KAFKA_IMG}.tgz
mkdir -p opt && echo "Created opt directory"
tar -xzf ${KAFKA_IMG}.tgz -C opt && rm ${KAFKA_IMG}.tgz && echo "Extracted $KAFKA_IMG"

cat >> opt/${KAFKA_IMG}/config/server.properties << --

advertised.host.name=localhost
advertised.host.port=9092
auto.create.topics.enable=false
delete.topic.enable=true
--

# drop and create main cluster, because it was created with locale=C...
pg_dropcluster --stop ${PGVERSION} main
pg_createcluster ${PGVERSION} main

CONFIG_DIR="/etc/postgresql/${PGVERSION}/main"
echo "listen_addresses = '*'" >> ${CONFIG_DIR}/postgresql.conf
echo "host all all 0.0.0.0/0 md5" >> ${CONFIG_DIR}/pg_hba.conf
