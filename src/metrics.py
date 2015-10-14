import os
import atexit
import time
from glob import glob
import collections
import flask

METRICS_FOLDER = os.environ.get('METRICS_FOLDER', '/tmp')

log = open('%s/metrics_%s.raw' % (METRICS_FOLDER, os.getpid()), 'w', 1)

def close_file():
    log.close()

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

            log.write('%s:%s:%s\n' % (fn_name, timestamp, response_status))

            return response

        return meter

    return measured_decorator


def aggregate_measures(cutoff_minutes):

    files = glob('%s/metrics_*.raw' % METRICS_FOLDER)

    call_count = collections.defaultdict(list)
    status_codes = {}
    cutoff_timestamp = time.time() - (cutoff_minutes * 60)

    for file_name in files:

        with open(file_name) as file:
            for current_line in file:
                line_values = current_line[:-1].split(':')
                function_name, timestamp, response_status = line_values[0], float(line_values[1]), line_values[2]
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
