#!/bin/bash

curl -v -X GET \
-H 'X-nakadi-cursors: [{"partition": "0", "offset": "3"}, {"partition": "5", "offset": "0"}, {"partition": "3", "offset": "20"}]' \
"http://localhost:8080/event-types/test-topic/events?batch_limit=5&batch_flush_timeout=10"