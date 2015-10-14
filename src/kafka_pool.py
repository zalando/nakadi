from contextlib import contextmanager
import logging
import queue
import threading
from kafka import KafkaClient
from kafka.common import KafkaError


class TooManyKafkaClientsException(Exception):
    pass


class KafkaClientPool(object):

    def __init__(self, kafka_brokers, init_pool_size, max_pool_size):
        self.queue = queue.Queue(max_pool_size)
        self.kafka_brokers = kafka_brokers
        self.released_clients = threading.BoundedSemaphore(value=max_pool_size)
        for _ in range(init_pool_size):
            self.queue.put(self.create_kafka_client())

    def create_kafka_client(self):
        logging.info("Creating kafka client (thread: %s)", threading.current_thread().getName())
        return KafkaClient(self.kafka_brokers)

    def __pick(self):
        logging.info("Pick (thread: %s)", threading.current_thread().getName())
        try:
            client = self.queue.get(block=False)
        except queue.Empty:
            client = self.create_kafka_client()
        return client

    def __put(self, client):
        logging.info("Put (thread: %s)", threading.current_thread().getName())
        return self.queue.put(client)

    @contextmanager
    def kafka_client(self):

        logging.info("Potential kafka clients left: %s", self.released_clients._value)
        if not self.released_clients.acquire(blocking=True, timeout=0.5):
            raise TooManyKafkaClientsException()

        client = None
        try:
            client = self.__pick()
            yield client
            self.__put(client)
        except KafkaError as kafka_error:
            try:
                client.close()
            except Exception as e:
                logging.warning("Exception when closing kafka client: %s", e)
            raise kafka_error
        except:
            self.__put(client)
        finally:
            self.released_clients.release()
