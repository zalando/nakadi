FROM zalando/python:3.4.0-2

COPY requirements.txt /
RUN pip3 install -r /requirements.txt

COPY src/*py /
COPY swagger.yaml /

WORKDIR /

ENV METRICS_FOLDER /tmp_metrics
RUN mkdir ${METRICS_FOLDER}
RUN chmod 777 ${METRICS_FOLDER}

# run with gunicorn to provide concurrency; params:
# - 32 workers (processes)
# - worker type: eventlet ("green threads")
# - log access_log to stderr
CMD gunicorn --workers 32 --worker-class eventlet --access-logfile - --bind 0.0.0.0:8080 hack:application