#!/usr/bin/env bash
curl -v "http://localhost:8080/subscriptions/sub1/events?batch_limit=5&batch_flush_timeout=10"