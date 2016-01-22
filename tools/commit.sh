#!/bin/bash

curl -v -X POST -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -d '[{ "topic": "test-topic", "partition": "0", "offset": "10"}, { "topic": "test-topic", "partition": "1", "offset": "10"}, { "topic": "test-topic", "partition": "2", "offset": "10"}, { "topic": "test-topic", "partition": "3", "offset": "10"}, { "topic": "test-topic", "partition": "4", "offset": "10"}, { "topic": "test-topic", "partition": "5", "offset": "10"}, { "topic": "test-topic", "partition": "6", "offset": "10"}, { "topic": "test-topic", "partition": "7", "offset": "10"}]' \
    "http://localhost:8080/subscriptions/sub1/cursors"