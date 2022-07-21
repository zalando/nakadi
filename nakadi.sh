#!/usr/bin/env bash

function waitForNakadi() {
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
  set -e
  ./gradlew clean
  ./gradlew :app:bootJar
  set +e
}

function help() {
  echo "Usage: ./nakadi.sh COMMAND"
  echo ""
  echo "Commands:"
  echo "  start-nakadi       build Nakadi and start docker-compose services: nakadi, postgresql, zookeeper and kafka"
  echo "  stop-nakadi        shutdown docker-compose services"
  echo "  start-storages     start docker-compose services: postgres, zookeeper and kafka (useful for development purposes)"
  echo "  stop-storages      shutdown docker-compose services"
  echo "  acceptance-tests   start Nakadi configured for acceptance tests and run acceptance tests"
  exit 1
}

COMMAND="${1}"
case $COMMAND in
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
*)
  help
  ;;
esac
