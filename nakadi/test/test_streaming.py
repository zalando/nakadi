#!/usr/bin/env python3
import json
import logging
import unittest

from nakadi.test import test_common
from nakadi.test.test_common import TEST_TOPIC, TEST_PARTITIONS_NUM
from nakadi.event_stream import BATCH_SEPARATOR

# test case for streaming endpoint
# for api version 0.3
class StreamingTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.hack = test_common.get_monkey_patched_hack()
        cls.app = cls.hack.conn_app.app.test_client()

        # bootstrap, so that each partition holds some events
        logging.info("Bootstraping events...")
        for i in range(TEST_PARTITIONS_NUM * 10):
            event = test_common.create_dummy_event(str(i))
            cls.app.post('/topics/%s/events' % TEST_TOPIC,
                     headers={'Content-type': 'application/json'},
                     data=json.dumps(event))

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'hack'):
            cls.hack.kafka_pool.close()

    def test_when_get_single_batch_then_ok(self):
        initial_offset = 0
        batch_size = 3
        response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                                query_string = {
                                    'batch_limit': batch_size,
                                    'stream_limit': batch_size,
                                    'batch_flush_timeout': '1',
                                    'stream_timeout': '1'
                                },
                                headers={'x-nakadi-cursors': '[{"partition": "0", "offset": "%s"}]' % initial_offset})
        print(response.data)
        assert response.status_code == 200
        self.__validate_batch_structure(json.loads(response.data.decode('utf-8')), initial_offset + batch_size, 0, batch_size)

    def test_when_get_multiple_batches_from_one_partition_then_ok(self):
        initial_offset = 0
        batch_size = 3
        num_batches = 2
        response = self.app.get('/topics/%s/events' % TEST_TOPIC,
                                query_string = {
                                    'batch_limit': batch_size,
                                    'stream_limit': batch_size * num_batches,
                                    'batch_flush_timeout': '1',
                                    'stream_timeout': '1'
                                },
                                headers={'x-nakadi-cursors': '[{"partition": "0", "offset": "%s"}]' % initial_offset})
        print(response.data)
        assert response.status_code == 200

        batches = response.data.decode('utf-8').split(BATCH_SEPARATOR + BATCH_SEPARATOR)
        batches.pop()

        assert len(batches) == num_batches
        for batch in batches:
            self.__validate_batch_structure(json.loads(batch), initial_offset + batch_size, 0, batch_size)
            initial_offset += batch_size

    def __validate_batch_structure(self, json_data, offset, partition, batch_size):
        assert 'cursor' in json_data
        print(json_data['cursor']['offset'])
        print(offset)
        assert json_data['cursor']['offset'] == str(offset)
        assert json_data['cursor']['partition'] == str(partition)
        assert 'events' in json_data
        assert len(json_data['events']) == batch_size

if __name__ == '__main__':
    unittest.main()