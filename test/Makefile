get_topics:
	curl --raw --verbose http://localhost:9000/topics

get_stream:
	curl --raw --verbose http://localhost:9000/topics/test-topic/partitions/0/events?start_from=0&stream_limit=3000


#	    -T -

put_stream:
	seq 1 3 | xargs -I % echo "{\"event_type\":\"test-event\", \"eid\": %}" \
	| curl --raw --verbose -i \
	    --data-binary @- \
	    -H "Content-Type: application/json-stream" \
	    -H "Transfer-Encoding: chunked" \
	    -X POST \
	    http://localhost:9000/topics/test-topic/events

put_events:
	echo "[{\"event_type\":\"test-event\", \"eid\": "1"}]" \
	| curl --raw --verbose -i \
	    --data-binary @- \
	    -H "Content-Type: application/json-stream" \
	    -H "Transfer-Encoding: chunked" \
	    -X POST \
	    http://localhost:9000/topics/test-topic/events
