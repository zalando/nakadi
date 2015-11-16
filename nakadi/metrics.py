import os
import atexit
import time
from glob import glob
import collections
import flask

METRICS_FOLDER = os.environ.get('METRICS_FOLDER', '/tmp')

endpoints_log = open('%s/endpoints_metrics_%s.raw' % (METRICS_FOLDER, os.getpid()), 'w', 1)
events_log = open('%s/events_metrics_%s.raw' % (METRICS_FOLDER, os.getpid()), 'w', 1)

def close_file():
    endpoints_log.close()
    events_log.close()

atexit.register(close_file)


def measured(fn_name):
    def measured_decorator(fn):
        def meter(*args, **kwargs):

            timestamp = time.time()
            response = fn(*args, **kwargs)

            if isinstance(response, flask.Response):
                response_status = response.status_code
            else:
                response_status = response[1]

            endpoints_log.write('%s:%s:%s\n' % (timestamp, fn_name, response_status))

            return response

        return meter

    return measured_decorator


def log_events(uid, topic, partition, events_pushed, events_consumed):
    events_log.write('%s:%s:%s:%s:%s:%s\n' % (time.time(), uid, topic, partition, events_pushed, events_consumed))


def aggregate_consumption_stats(cutoff_minutes):
    files = glob('%s/events_metrics_*.raw' % METRICS_FOLDER)
    cutoff_timestamp = time.time() - (cutoff_minutes * 60)
    stats = {}

    for file_name in files:

        with open(file_name) as file:
            for current_line in file:

                line_values = current_line[:-1].split(':')
                timestamp, uid, topic, partition, events_pushed, events_consumed = \
                    float(line_values[0]), line_values[1], line_values[2], line_values[3], int(line_values[4]), int(line_values[5])

                if timestamp > cutoff_timestamp:
                    if topic not in stats:
                        stats[topic] = {}
                        stats[topic]['pushed'] = {}
                        stats[topic]['consumed'] = {}
                    if events_pushed > 0:
                        if uid not in stats[topic]['pushed']:
                            stats[topic]['pushed'][uid] = 0
                        stats[topic]['pushed'][uid] += events_pushed
                    if events_consumed > 0:
                        if uid not in stats[topic]['consumed']:
                            stats[topic]['consumed'][uid] = 0
                        stats[topic]['consumed'][uid] += events_consumed
    return stats


def aggregate_endpoints_stats(cutoff_minutes):

    files = glob('%s/endpoints_metrics_*.raw' % METRICS_FOLDER)

    call_count = collections.defaultdict(list)
    status_codes = {}
    cutoff_timestamp = time.time() - (cutoff_minutes * 60)

    for file_name in files:

        with open(file_name) as file:
            for current_line in file:
                line_values = current_line[:-1].split(':')
                timestamp, function_name, response_status = float(line_values[0]), line_values[1], line_values[2]
                if timestamp > cutoff_timestamp:
                    call_count[function_name].append(timestamp)

                    if function_name not in status_codes:
                        status_codes[function_name] = {}
                    if response_status not in status_codes[function_name]:
                        status_codes[function_name][response_status] = 0
                    status_codes[function_name][response_status] += 1

    stats = {}
    for function_name, timestamp_list in call_count.items():
        stats[function_name] = {
            'count': len(timestamp_list),
            'calls_per_second': len(timestamp_list) / (cutoff_minutes * 60),
            'status_codes': status_codes[function_name]
        }

    return stats
