from contextlib import contextmanager
import datetime
import json
import logging
from kafka import KafkaConsumer
from kafka.common import ConsumerTimeout
import sys


def create_stream_generator(kafka_pool, topic, partition, start_from, opts):

    partitions_offsets = [(0, 0), (1, 0), (2, 0)]

    topics = {}
    for partition_offset in partitions_offsets:
        topics[(topic, partition_offset[0])] = partition_offset[1]

    partitions = [partition_offset[0] for partition_offset in partitions_offsets]

    start = datetime.datetime.now()

    # stream
    def generator():

        keep_alive_in_a_row = 0

        messages_read =  0
        current_batch = dict(zip(partitions, [[] for partition in partitions]))

        batch_start_time = datetime.datetime.now()
        with kafka_pool.kafka_client() as client:

            consumer = KafkaConsumer(topics,
                                     kafka_client=client,
                                     auto_commit_enable=False,
                                     consumer_timeout_ms=200,
                                     fetch_message_max_bytes= 1024) # 128 kb max batch size for topic/partition

            while True:
                try:
                    message = consumer.next()
                    # if we read the message - reset keep alive counter
                    keep_alive_in_a_row = 0

                    # put message to batch
                    messages_read += 1
                    message_json = json.loads(message.value.decode('utf-8'))
                    current_batch[message.partition].append(message_json)

                except ConsumerTimeout:
                    pass

                # check if it's time to send the batch
                time_since_batch_start = datetime.datetime.now() - batch_start_time
                max_batch_size = max([len(batch_per_partition) for batch_per_partition in current_batch.values()])
                latest_offsets = consumer.offsets("fetch")
                if time_since_batch_start.total_seconds() >= opts['batch_flush_timeout'] != 0 or max_batch_size >= opts['batch_limit']:
                    for partition in partitions:
                        topic_partition = (topic.encode('UTF-8'), partition)
                        # send the messages we could read so far
                        if len(current_batch[partition]) > 0:
                            stream_message = __create_stream_message(partition, latest_offsets[topic_partition], current_batch[partition])
                            with __measure_time(current_batch[partition], stream_message):
                                yield stream_message

                        # just send the keep alive
                        else:
                            yield __create_stream_message(partition, latest_offsets[topic_partition])

                    # if we hit keep alive count limit - close the stream
                    if max_batch_size == 0:
                        if keep_alive_in_a_row >= opts['batch_keep_alive_limit'] != -1:
                            break
                        keep_alive_in_a_row += 1

                    # init new batch
                    current_batch = dict(zip(partitions, [[] for _ in partitions]))
                    batch_start_time = datetime.datetime.now()

                    yield '\n\n\n'

                # check if we reached the stream timeout or message count limit
                time_since_start = datetime.datetime.now() - start
                if time_since_start.total_seconds() >= opts['stream_timeout'] > 0 or 0 < opts['stream_limit'] <= messages_read:

                    if max_batch_size > 0:
                        for partition in partitions:
                            topic_partition = (topic.encode('UTF-8'), partition)
                            # send the messages we could read so far
                            if len(current_batch[partition]) > 0:

                                stream_message = __create_stream_message(partition, latest_offsets[topic_partition], current_batch[partition])
                                with __measure_time(current_batch[partitions], stream_message):
                                    yield stream_message

                            # just send the keep alive
                            else:
                                yield __create_stream_message(partition, latest_offsets[topic_partition])
                    break

    return generator()


def __create_stream_message(partition, offset, events = None, topology = None):
    message = {
        'cursor': {
            'partition': str(partition),
            'offset': str(offset)
        }
    }
    if events is not None:
        message['events'] = events
    if topology is not None:
        message['topology'] = topology
    return json.dumps(message) + '\n'


@contextmanager
def __measure_time(batch, message):

    message_size_bytes = sys.getsizeof(message)
    batch_length = len(batch)

    before = datetime.datetime.now()

    yield

    after = datetime.datetime.now()
    delta =  after - before
    ms = int(delta.total_seconds() * 1000)

    logging.info("[#BAND_CHECK] Batch size: %s; Size in bytes: %s; Ms spent on yielding: %s", batch_length, message_size_bytes, ms)
