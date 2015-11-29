FROM zalando/python:3.4.0-2

COPY requirements.txt /
RUN pip3 install -r /requirements.txt

# copy project code and gunicorn config
ADD nakadi /nakadi
COPY gunicorn_conf.py /
# remove python cache files
RUN find ./nakadi | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf

WORKDIR /

ENV METRICS_FOLDER /tmp_metrics
RUN mkdir ${METRICS_FOLDER}
RUN chmod 777 ${METRICS_FOLDER}

# run with gunicorn to provide concurrency
CMD gunicorn -c gunicorn_conf.py nakadi.hack:application