# gunicorn configuration file
import logging

# 32 workers (processes)
workers = 2

# worker type: eventlet ("green threads")
worker_class = 'eventlet'

# do access-logging to stderr
accesslog = '-'

# bind address/port
bind = '0.0.0.0:8080'


# request hooks definitions
def dump_request_with_body(worker, req):
    try:
        # currently we need this only for push endpoint(s)
        if req.path and req.path.startswith('/topics/') and req.path.endswith('/events') and req.method == 'POST':
            logging.info('[pre_request] request:\n%s', req.__dict__)
            logging.info('[pre_request] body:\n%s', req.unreader.buf.getvalue().decode("utf-8"))
    except:
        logging.warning('Error occurred while trying to dump request')


def dump_response_with_environment(worker, req, environ, resp):
    try:
        # we need to dump the response only in a case of error
        if resp.status_code and (resp.status_code < 200 or resp.status_code >= 300):
            logging.info('[post_request] response:\n%s', resp.__dict__)
            logging.info('[post_request] environment:\n%s', environ)
    except:
        logging.warning('Error occurred while trying to dump response')


pre_request = dump_request_with_body
post_request = dump_response_with_environment

logging.basicConfig(level=logging.INFO)
