#!/bin/bash
for i in `seq 1 100`; do
	curl -sL -w "%{http_code}\\n" -H "Content-Type: application/json" -XPOST "localhost:8080/event-types/order.ORDER_RECEIVED9/events" -d '[{"order_number":"12", "metadata":{"eid":"d765de34-09c0-4bbb-8b1e-7160a33a0791", "occurred_at":"2016-03-15T23:47:15+01:00"}}]' -o /dev/null
done
