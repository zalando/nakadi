#!/bin/bash

GUNICORN_WORKERS=${GUNICORN_WORKERS:-$((2*$(nproc)+1))}

exec gunicorn --workers ${GUNICORN_WORKERS} --worker-class eventlet --access-logfile - --error-logfile - --log-level debug --bind 0.0.0.0:8080 nakadi.hack:application
