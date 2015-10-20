import logging
import re
import threading
import tokens
import json
import requests
import monitoring
from kafka import KafkaClient, KeyedProducer, SimpleConsumer, SimpleProducer
from kafka.common import UnknownTopicOrPartitionError
from time import sleep
import traceback

NO_SUBSCRIPTION_WAIT_PERIOD_S = 5
NO_MESSAGES_WAIT_PERIOD_S = 2

def normalize_consumer(topic, callback):
    """
    >>> normalize_consumer('', '')
    ':'
    >>> normalize_consumer('asdf-asdf.qwer', 'http://fuu/bar?eoriu=&twert=234')
    'asdf-asdf.qwer:http___fuu_bar_eoriu__twert_234'
"""

    return '%s:%s' % (topic, re.sub(r'[:/?=&]', '_', callback))


class BackgroundThread(threading.Thread):

    def __init__(self, kafka_broker, metrics, callback_store, worker_thread_count=1, max_read_messages_per_cycle=1, use_oauth2_for_push=False):
        threading.Thread.__init__(self, daemon=True)
        self.use_oauth2_for_push = use_oauth2_for_push
        self.callback_store = callback_store
        self.worker_thread_count = worker_thread_count
        self.max_read_messages_per_cycle = max_read_messages_per_cycle
        self.kafka = kafka_broker
        self.metrics = metrics
        if use_oauth2_for_push:
            tokens.manage('event-store', ['uid'])
            tokens.start()

    def forward_event(self, callback_url, event, topic):
        logging.debug('Forwarding to %s', callback_url)
        headers = {'Content-Type': 'application/json'}

        if self.use_oauth2_for_push:
            headers['Authorization'] = 'Bearer {}'.format(tokens.get('event-store'))
        response = requests.post(callback_url, data=json.dumps(event), headers=headers)

        self.metrics['events_out'].inc({'topic': topic, 'uuid': '0', 'url': callback_url, 'status_code': int(response.status_code)})

        print(response.status_code, response.text)
        return int(response.status_code)

    def run(self):

        # the "push" function
        def consume_topic(callback_url, consumer_group, topic):
            consumer = None
            try:
                consumer = SimpleConsumer(self.kafka, consumer_group, topic, auto_commit=False)
                messages_read = 0

                # we can't read messages infinitely here as we have
                # a lot of topics/subscribers (much more than threadpool size)
                while messages_read < self.max_read_messages_per_cycle:

                    # get one message and monitor the time
                    start = monitoring.start_time_measure()
                    message = consumer.get_message(block=False)
                    ms_elapsed = monitoring.stop_time_measure(start)
                    self.metrics['kafka_read'].add({'topic': topic}, ms_elapsed)

                    # if we don't have messages for this topic/subscriber - quit and give chance to others
                    if message is None:
                        logging.info('No messages for topic: %s and callback: %s, quiting the thread', topic, callback_url)
                        break

                    try:
                        event = json.loads(message.message.value.decode('utf-8'))
                        response_status = self.forward_event(callback_url, event, topic)

                        # if status is success - mark message as consumed by this subscriber
                        if 200 <= response_status < 300:
                            consumer.commit()
                        else:
                            logging.info('Received error response fro consumer: %s', response_status)
                    except:
                        logging.error("Exception while sending event to consumer")
                        logging.error(traceback.format_exc())
                    finally:
                        messages_read += 1
                return messages_read

            except UnknownTopicOrPartitionError:
                logging.error('Adding %s to skip list', topic)
            except:
                logging.exception('failed to create kafka client')
            finally:
                if consumer is not None:
                    consumer.stop()


        from concurrent.futures import ThreadPoolExecutor

        logging.info('Starting background thread')

        while True:
            try:
                topics = self.callback_store.get_currently_subscribed_topics()
                logging.info('Got callback_store: %s', topics)

                # if we don't have any subscription - sleep some time as we
                # don't want to fetch too often without having subscribers
                if len(topics) == 0:
                    logging.info('No subscriptions; sleeping for %s', NO_SUBSCRIPTION_WAIT_PERIOD_S)
                    sleep(NO_SUBSCRIPTION_WAIT_PERIOD_S)
                    continue

                results = []
                with ThreadPoolExecutor(max_workers=self.worker_thread_count) as executor:

                    for topic_id in topics:
                        logging.debug('Processing topic %s', topic_id)
                        if not topic_id:
                            continue
                        topic_subscribers = self.callback_store.get(topic_id)
                        for callback_url, consumer_group in map(lambda subscriber: (subscriber['callback'], normalize_consumer(topic_id, subscriber['callback'])), topic_subscribers):
                            try:
                                results.append(executor.submit(consume_topic, callback_url, consumer_group, topic_id))
                            except:
                                logging.error('Error when submiting thread for execution [#TSFAIL]')
                                logging.error(traceback.format_exc())

                # join threads
                sum_of_messages = 0
                for result in results:
                    if result is not None and isinstance(result.result(), int):
                        sum_of_messages += result.result()

                # if we didn't get any messages - sleep a little till the next check
                if sum_of_messages == 0:
                    logging.info('No messages; sleeping for %s seconds', NO_MESSAGES_WAIT_PERIOD_S)
                    sleep(NO_MESSAGES_WAIT_PERIOD_S)
                else:
                    logging.info('Read %s messages in last run', sum_of_messages)

            except:
                logging.error('Error when running background thread [#BGFAIL]')
                logging.error(traceback.format_exc())


