#!/usr/bin/env python3
import json
import logging
import unittest
import test_common
from test_common import TEST_TOPIC, TEST_PARTITIONS_NUM

# test case for streaming endpoint
# for api version 0.3
class StreamingTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.hack = test_common.get_monkey_patched_hack()
        cls.app = cls.hack.application.test_client()

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

    def test_when_get_single_event_then_ok(self):
        initial_offset = 0
        batch_size = 3
        response = self.app.get('/topics/%s/partitions/0/events' % TEST_TOPIC,
                                query_string = {
                                    'start_from': initial_offset,
                                    'batch_limit': batch_size,
                                    'stream_limit': batch_size,
                                    'batch_flush_timeout': '1',
                                    'stream_timeout': '1'
                                })
        assert response.status_code == 200

        data = json.loads(response.data.decode('utf-8'))
        assert 'cursor' in data
        assert data['cursor']['offset'] == str(initial_offset + batch_size)
        assert data['cursor']['partition'] == '0'

        assert 'events' in data
        assert len(data['events']) == 3

if __name__ == '__main__':
    unittest.main()