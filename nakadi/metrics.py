import logging
import os
import atexit
import random
import threading
import time
from glob import glob
import collections
import traceback

import flask

from nakadi import config

METRICS_FOLDER = os.environ.get('METRICS_FOLDER', '/tmp')
ENDPOINTS_FILE_BASE_NAME = 'endpoints_metrics_'
EVENTS_FILE_BASE_NAME = 'events_metrics_'

METRICS_LOG_TTL_MINUTES = 60
METRICS_LOG_ROTATE_MINUTES = 20


class MetricsWriter:

    def __init__(self):
        logging.info('Metrics logs will be rotated every %s minutes' % METRICS_LOG_ROTATE_MINUTES)
        logging.info('Metrics logs will be removed %s minutes after creation' % METRICS_LOG_TTL_MINUTES)

        self.__create_log_files()
        if config.ENABLE_LOGS_ROTATION:
            self.__schedule_logs_rotation()
        atexit.register(self.__close_current_files)

    def __schedule_logs_rotation(self):
        threading.Timer(METRICS_LOG_ROTATE_MINUTES * 60, self.__rotate_log_files).start()

    def __close_current_files(self):
        self.endpoints_log.close()
        self.events_log.close()

    def __close_old_log_files(self):
        self.old_event_log.close()
        self.old_endpoint_log.close()

    def __generate_log_file_name(self, filename_mask):
        return filename_mask % (METRICS_FOLDER, os.getpid(), random.randint(0, 1000000))

    def __create_log_files(self):
        self.endpoints_log = open(self.__generate_log_file_name('%s/' + ENDPOINTS_FILE_BASE_NAME + '%s_%s.raw'), 'w', 1)
        self.events_log = open(self.__generate_log_file_name('%s/' + EVENTS_FILE_BASE_NAME + '%s_%s.raw'), 'w', 1)

    def __rotate_log_files(self):
        logging.info('[MLROTATE] Performing metrics logs rotation')
        try:
            # remove old files created by this process
            files = glob('%s/%s%s*.raw' % (METRICS_FOLDER, ENDPOINTS_FILE_BASE_NAME, os.getpid()))
            files.extend(glob('%s/%s%s*.raw' % (METRICS_FOLDER, EVENTS_FILE_BASE_NAME, os.getpid())))
            for file_name in files:
                creation_time = os.path.getctime(file_name)
                created_seconds_ago = time.time() - creation_time
                if created_seconds_ago > METRICS_LOG_TTL_MINUTES * 60:
                    os.remove(file_name)
                    logging.info('[MLROTATE] removed file: %s created %s seconds ago' % (file_name, created_seconds_ago))
                else:
                    logging.info('[MLROTATE] skipped file %s created %s seconds ago' % (file_name, created_seconds_ago))

            # rotate current log files
            self.old_event_log = self.events_log
            self.old_endpoint_log = self.endpoints_log
            self.__create_log_files()
            # close old log files after small delay
            threading.Timer(2, self.__close_old_log_files).start()
        except:
            logging.warning('[MLROTATE] Error occurred when rotating metrics logs:')
            traceback.print_exc()

        # schedule next execution
        self.__schedule_logs_rotation()

    def measured(self, fn_name):
        def measured_decorator(fn):
            def meter(*args, **kwargs):
                timestamp = time.time()
                response = fn(*args, **kwargs)
                if isinstance(response, flask.Response):
                    response_status = response.status_code
                else:
                    response_status = response[1]
                self.endpoints_log.write('%s:%s:%s\n' % (timestamp, fn_name, response_status))
                return response
            return meter
        return measured_decorator

    def log_events(self, uid, topic, partition, events_pushed, events_consumed):
        self.events_log.write('%s:%s:%s:%s:%s:%s\n' % (time.time(), uid, topic, partition, events_pushed, events_consumed))

    def aggregate_consumption_stats(self, cutoff_minutes):
        files = glob('%s/%s*.raw' % (METRICS_FOLDER, EVENTS_FILE_BASE_NAME))
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

    def aggregate_endpoints_stats(self, cutoff_minutes):

        files = glob('%s/%s*.raw' % (METRICS_FOLDER, ENDPOINTS_FILE_BASE_NAME))

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

metrics_writer = MetricsWriter()
measured = metrics_writer.measured
