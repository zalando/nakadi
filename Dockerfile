FROM zalando/python:3.4.0-2

COPY requirements.txt /
RUN pip3 install -r /requirements.txt

# copy project code
ADD nakadi /nakadi
# remove python cache files
RUN find ./nakadi | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf

ADD run.sh /nakadi/run.sh

WORKDIR /

ENV METRICS_FOLDER /tmp_metrics
RUN mkdir ${METRICS_FOLDER}
RUN chmod 777 ${METRICS_FOLDER}

CMD ["bash", "/nakadi/run.sh"]
