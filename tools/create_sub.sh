#!/bin/bash

curl -v -X POST -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -d '["test-topic"]' \
    "http://localhost:8080/subscriptions/sub1"