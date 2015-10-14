#!/usr/bin/env python3
import datetime

from prometheus.collectors import Counter
from prometheus.collectors import Summary

def init_metrics(registry):
    events_in_metric = Counter('events_in', 'Events received')
    events_out_metric = Counter('events_out', 'Events published')
    kafka_write = Summary('kafka_write', 'Kafka write time')
    kafka_read = Summary('kafka_read', 'Kafka read time')

    registry.register(events_in_metric)
    registry.register(events_out_metric)
    registry.register(kafka_write)
    registry.register(kafka_read)

    return {
        'events_in': events_in_metric,
        'events_out': events_out_metric,
        'kafka_write': kafka_write,
        'kafka_read': kafka_read
    }

def to_json(metrics):
    return {
        'events_in': metrics['events_in'].get_all(),
        'subscriptions': metrics['events_out'].get_all(),
        'kafka_write': metrics['kafka_write'].get_all(),
        'kafka_read': metrics['kafka_read'].get_all()
    }

def start_time_measure():
    return datetime.datetime.now()

def stop_time_measure(datetime_before):
    datetime_after = datetime.datetime.now()
    delta = datetime_after - datetime_before
    ms = int(delta.total_seconds() * 1000)
    return ms

