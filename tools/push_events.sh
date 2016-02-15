#!/bin/bash

# push 5 events to each of 8 partitions
for p in {0..7}
do
    for i in {1..5}
    do
        curl -v -X POST -H "Accept: application/json" \
            -H "Content-Type: application/json" \
            -d '"Dummy"' \
            "http://localhost:8080/event-types/test-topic/events" &
    done
    wait
done

