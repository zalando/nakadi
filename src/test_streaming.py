#!/usr/bin/env python3
import json
import logging
import unittest
import test_common
from test_common import TEST_TOPIC, TEST_PARTITIONS_NUM

# this test case needs some events to be contained in each partition
# for api version 0.3
class EventstoreStreamingTestCase(unittest.TestCase):


    def setUp(self):
        self.app = test_common.get_mokey_patched_app().test_client()


    def tearDown(self):
        pass


    def test_when_get_topics_then_ok(self):
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
        print(data)

        assert 'cursor' in data
        assert data['cursor']['offset'] == str(initial_offset + batch_size)
        assert data['cursor']['partition'] == '0'

        assert 'events' in data
        assert len(data['events']) == 3


def __bootstrap_events():
    logging.info("Bootstraping events...")
    app = test_common.get_mokey_patched_app().test_client()

    for i in range(TEST_PARTITIONS_NUM * 10):
        event = test_common.create_dummy_event(str(i))
        app.post('/topics/%s/events' % TEST_TOPIC,
                 headers={'Content-type': 'application/json'},
                 data=json.dumps(event))


if __name__ == '__main__':
    __bootstrap_events()
    unittest.main()