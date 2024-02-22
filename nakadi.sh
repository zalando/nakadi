#!/bin/bash -e

function waitForNakadi() {
  set +e
  echo "Waiting for Nakadi to start up"
  x=1
  while [[ x -lt 10 ]]; do
    res=$(curl -s -w "%{http_code}" -o /dev/null http://localhost:8080/health)
    if ((res == 200)); then
      echo "Nakadi is fully started"
      return
    fi
    echo "Nakadi boots up"
    sleep 10
    x=$((x + 1))
  done
  set -e
}

function startNakadi() {
  export SPRING_PROFILES_ACTIVE=local
  docker-compose up -d --build
  waitForNakadi
}

function stopNakadi() {
  docker-compose down
}

function startStorages() {
  docker-compose up -d postgres zookeeper kafka
}

function acceptanceTests() {
  export SPRING_PROFILES_ACTIVE=acceptanceTest
  docker-compose up -d --build
  waitForNakadi
  if ./gradlew :acceptance-test:acceptanceTest
  then
      errcode=0
  else
      errcode=1
      docker-compose logs nakadi
  fi
  docker-compose down
  return $errcode
}

function buildNakadi() {
  ./gradlew clean app:bootJar authz:fatJar lightstep:fatJar
}

function createBuildx() {
  docker buildx rm -f cdpbuildx || true
  docker buildx create \
    --config "$1" \
    --driver-opt network=host \
    --name cdpbuildx \
    --bootstrap \
    --use
}

function buildBuildx() {
  docker buildx build --platform linux/amd64,linux/arm64 -t "$1" --push .
}

function help() {
  echo "Usage: ./nakadi.sh COMMAND"
  echo ""
  echo "Commands:"
  echo "  clean-build        clean build of Nakadi"
  echo "  start-nakadi       build Nakadi and start docker-compose services: nakadi, postgresql, zookeeper and kafka"
  echo "  stop-nakadi        shutdown docker-compose services"
  echo "  start-storages     start docker-compose services: postgres, zookeeper and kafka (useful for development purposes)"
  echo "  stop-storages      shutdown docker-compose services"
  echo "  acceptance-tests   start Nakadi configured for acceptance tests and run acceptance tests"
  echo "  create-buildx      create docker buildx container for building multi-architecture images"
  echo "  build-buildx       build docker multi-architecture image (use create-buildx first)"
  exit 1
}

COMMAND="${1}"
case $COMMAND in
clean-build)
  buildNakadi
  ;;
start-nakadi)
  buildNakadi
  startNakadi
  ;;
stop-nakadi)
  stopNakadi
  ;;
start-storages)
  startStorages
  ;;
stop-storages)
  stopStorages
  ;;
acceptance-tests)
  buildNakadi
  acceptanceTests
  ;;
create-buildx)
  if [ $# != 2 ]; then
    echo "Usage: nakadi.sh create-buildx CONFIG_FILE"
    exit 1
  fi
  createBuildx "$2"
  ;;
build-buildx)
  if [ $# != 2 ]; then
    echo "Usage: nakadi.sh build-buildx IMAGE"
    exit 1
  fi
  buildBuildx "$2"
  ;;
*)
  help
  ;;
esac
