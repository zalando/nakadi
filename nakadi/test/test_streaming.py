#!/usr/bin/env python3
import json
import logging

from nakadi.test import test_common
from nakadi.test.test_common import TEST_TOPIC, TEST_PARTITIONS_NUM, validate_error_response
from nakadi.event_stream import BATCH_SEPARATOR

# test case for streaming endpoint
# for api version 0.3
class TestStreamingEndpoint:

    @classmethod
    def setup_class(cls):
        cls.hack = test_common.get_monkey_patched_hack()
        cls.app = cls.hack.conn_app.app.test_client()

        # bootstrap, so that each partition holds some events
        logging.info("Bootstraping events...")
        for i in range(TEST_PARTITIONS_NUM * 20):
            event = test_common.create_dummy_event(str(i))
            cls.app.post('/topics/%s/events' % TEST_TOPIC,
                     headers={'Content-type': 'application/json'},
                     data=json.dumps(event))

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'hack'):
            cls.hack.kafka_pool.close()

    def test_when_get_single_batch_then_ok(self):
        initial_offset = 0
        batch_size = 3
        response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                                query_string = {
                                    'batch_limit': batch_size,
                                    'stream_limit': batch_size
                                },
                                headers={'x-nakadi-cursors': '[{"partition": "0", "offset": "%s"}]' % initial_offset})
        assert response.status_code == 200
        self.__validate_batch(json.loads(response.data.decode('utf-8')), initial_offset + batch_size, 0, batch_size)

    def test_when_get_multiple_batches_from_one_partition_then_ok(self):
        initial_offset = 0
        batch_size = 3
        num_batches = 2
        response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                                query_string = {
                                    'batch_limit': batch_size,
                                    'stream_limit': batch_size * num_batches
                                },
                                headers={'x-nakadi-cursors': '[{"partition": "0", "offset": "%s"}]' % initial_offset})
        assert response.status_code == 200

        batches = response.data.decode('utf-8').split(BATCH_SEPARATOR + BATCH_SEPARATOR)
        batches.pop() #the last piece is just an empty string, we don't need it

        assert len(batches) == num_batches
        for batch in batches:
            self.__validate_batch(json.loads(batch), initial_offset + batch_size, 0, batch_size)
            initial_offset += batch_size

    def test_when_get_single_batch_from_multiple_partitions_then_ok(self):
        initial_offset = 0
        batch_size = 3
        response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                                query_string = {
                                    'batch_limit': batch_size,
                                    'stream_limit': batch_size
                                },
                                headers={'x-nakadi-cursors': '[{"partition": "0", "offset": "%s"}, {"partition": "1", "offset": "%s"}]'
                                             % (initial_offset, initial_offset)})
        assert response.status_code == 200

        batch = response.data.decode('utf-8')
        partition_batches = batch.split(BATCH_SEPARATOR)
        # remove two empty strings at the end
        partition_batches.pop()
        partition_batches.pop()
        assert len(partition_batches) == 2

        events_total = 0
        for partition_batch in partition_batches:
            batch_json = json.loads(partition_batch)
            self.__validate_batch_structure(batch_json)
            if 'events' in batch_json:
                events_total += len(batch_json['events'])
        # check that total number of events in all partitions equals to batch size
        assert events_total == batch_size

    def test_when_get_multiple_batches_from_multiple_partitions_then_ok(self):
        initial_offset = 0
        batch_size = 3
        num_batches = 2
        response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                                query_string = {
                                    'batch_limit': batch_size,
                                    'stream_limit': batch_size * num_batches
                                },
                                headers={'x-nakadi-cursors': '[{"partition": "0", "offset": "%s"}, {"partition": "1", "offset": "%s"}]'
                                             % (initial_offset, initial_offset)})
        assert response.status_code == 200

        batches = response.data.decode('utf-8').split(BATCH_SEPARATOR + BATCH_SEPARATOR)
        batches.pop() #the last piece is just an empty string, we don't need it

        assert len(batches) == num_batches
        for batch in batches:
            partition_batches = batch.split(BATCH_SEPARATOR)
            events_total = 0
            for partition_batch in partition_batches:
                batch_json = json.loads(partition_batch)
                self.__validate_batch_structure(batch_json)
                if 'events' in batch_json:
                    events_total += len(batch_json['events'])
            # check that total number of events in all partitions equals to batch size
            assert events_total == batch_size

    def test_when_get_events_without_cursors_then_stream_from_all_partitions_from_latest_offset(self):
        batch_size = 1
        response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                                query_string = {
                                    'batch_limit': batch_size,
                                    'stream_limit': batch_size,
                                    'batch_flush_timeout': '1',
                                    'stream_timeout': '1'
                                })
        assert response.status_code == 200
        batch = response.data.decode('utf-8')
        partition_batches = batch.split(BATCH_SEPARATOR)
        # remove two empty strings at the end
        partition_batches.pop()
        partition_batches.pop()

        # validate that we stream from all partitions
        assert len(partition_batches) == TEST_PARTITIONS_NUM

        response = self.app.get('/topics/%s/partitions' % TEST_TOPIC)
        partitions = json.loads(response.data.decode('utf-8'))

        total_events = 0
        # validate that we stream from latest offsets
        for partition_batch in partition_batches:
            batch_json = json.loads(partition_batch)
            self.__validate_batch_structure(batch_json)
            partition = batch_json['cursor']['partition']
            latest_offset = [str(part['newest_available_offset']) for part in partitions if str(part['partition_id']) == partition][0]
            assert batch_json['cursor']['offset'] == latest_offset

            if 'events' in batch_json:
                total_events += len(batch_json['events'])

        # validate that we have not read any messages as we were reading from latest offsets
        assert total_events == 0

    def test_when_get_events_with_invalid_nakadi_cursors_then_bad_syntax(self):
        headers_set = [
            'partition=0&offset=0', # not a json
            '{"partition": "0", "offset": "0"}', # not a list
            '[{"partition": "0", "new_offset": "0"}]', # doesn't have 'offset' field
            '[{"part": "0", "offset": "0"}]', # doesn't have 'partition' field
            '[{"partition": "a", "offset": "0"}]', # partition is not a number
            '[{"partition": "0", "offset": "a"}]' # offset is not a number
        ]
        for cursors in headers_set:
            response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                                headers={'x-nakadi-cursors': cursors})
            validate_error_response(response, 400, '"x-nakadi-cursors" header has wrong format')

    def test_when_get_events_with_query_parameter_not_number_then_bad_syntax(self):
        query_params = [
            'batch_limit',
            'batch_flush_timeout',
            'batch_keep_alive_limit',
            'stream_limit',
            'stream_timeout'
        ]
        for param in query_params:
            response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                                query_string = {
                                    param: 'a'
                                })
            assert response.status_code == 400

    def test_when_get_events_with_unknown_topic_then_topic_not_found(self):
        response = self.app.get('/topics/dummy-topic/events')
        validate_error_response(response, 404, 'topic not found')

    def test_when_get_events_with_invalid_patition_then_partition_not_found(self):
        response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                            headers={'x-nakadi-cursors': '[{"partition": "%s", "offset": "0"}]' % TEST_PARTITIONS_NUM})
        validate_error_response(response, 404, 'partition not found')

    def __validate_batch(self, batch_json, offset, partition, batch_size):
        self.__validate_batch_structure(batch_json)
        self.__validate_batch_content(batch_json, offset, partition, batch_size)

    def __validate_batch_structure(self, batch_json):
        assert 'cursor' in batch_json
        assert 'offset' in batch_json['cursor']
        assert 'partition' in batch_json['cursor']

    def __validate_batch_content(self, batch_json, offset, partition, batch_size):
        assert batch_json['cursor']['offset'] == str(offset)
        assert batch_json['cursor']['partition'] == str(partition)
        assert len(batch_json['events']) == batch_size
