#!/usr/bin/env python3
import json

from nakadi.test import test_common
from nakadi.test.test_common import TEST_TOPIC, TEST_PARTITIONS_NUM, validate_error_response


# test case for nakadi endpoints (except streaming endpoint)
# for api version 0.3
class TestNakadiEndpoints:

    @classmethod
    def setup_class(cls):
        cls.hack = test_common.get_monkey_patched_hack()
        cls.app = cls.hack.conn_app.app.test_client()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'hack'):
            cls.hack.kafka_pool.close()

    def test_when_health_check_then_ok(self):
        response = self.app.get('/health')
        assert response.status_code == 200

    def test_when_get_topics_then_ok(self):
        response = self.app.get('/topics')
        assert response.status_code == 200

        topics = json.loads(response.data.decode('utf-8'))
        assert len(topics) == 1
        assert 'name' in topics[0]
        assert topics[0]['name'] == TEST_TOPIC

    def test_when_get_partitions_then_ok(self):
        response = self.app.get('/topics/%s/partitions' % TEST_TOPIC)
        assert response.status_code == 200

        partitions = json.loads(response.data.decode('utf-8'))
        assert len(partitions) == TEST_PARTITIONS_NUM

        for partition in partitions:
            self.__validate_partition_structure(partition)

    def test_when_get_partition_then_ok(self):
        response = self.app.get('/topics/%s/partitions/0' % TEST_TOPIC)
        assert response.status_code == 200

        partition = json.loads(response.data.decode('utf-8'))
        self.__validate_partition_structure(partition)

    def test_when_get_partition_for_not_existing_topic_then_topic_not_found(self):
        response = self.app.get('/topics/blahtopic/partitions/0')
        validate_error_response(response, 404, 'topic not found')

    def test_when_get_not_existing_partition_then_partition_not_found(self):
        response = self.app.get('/topics/%s/partitions/2341' % TEST_TOPIC)
        validate_error_response(response, 404, 'partition not found')

    def test_when_get_letter_partition_then_partition_not_a_number(self):
        response = self.app.get('/topics/%s/partitions/ab' % TEST_TOPIC)
        validate_error_response(response, 400, '"partition" path parameter should be an integer number')

    def test_when_get_partitions_for_not_existing_topic_then_topic_not_found(self):
        response = self.app.get('/topics/not_existing_topic/partitions')
        validate_error_response(response, 404, 'topic not found')

    def test_when_post_event_then_ok(self):
        response = self.app.post('/topics/%s/events' % TEST_TOPIC,
                                 headers = {'Content-type': 'application/json'},
                                 data = json.dumps(test_common.create_dummy_event('dummy-key')))
        assert response.status_code == 201

    def test_when_post_event_then_newest_offset_in_one_partition_was_increased(self):
        # get initial offsets
        response = self.app.get('/topics/%s/partitions' % TEST_TOPIC)
        initial_partitions_offsets = json.loads(response.data.decode('utf-8'))

        # post message
        self.test_when_post_event_then_ok()

        # get new partitions offsets
        response = self.app.get('/topics/%s/partitions' % TEST_TOPIC)
        new_partitions_offsets = json.loads(response.data.decode('utf-8'))

        # check that newest offset in one of the partitions was increased
        number_of_partitions_with_increased_offset = 0
        for initial_offset in initial_partitions_offsets:
            new_offset = [partition for partition in new_partitions_offsets if partition.get('partition_id') == initial_offset['partition_id']][0]
            if new_offset['newest_available_offset'] > initial_offset['newest_available_offset']:
                assert new_offset['newest_available_offset'] - initial_offset['newest_available_offset'] == 1
                number_of_partitions_with_increased_offset += 1

        assert number_of_partitions_with_increased_offset == 1

    def __validate_partition_structure(self, partition):
        assert 'partition_id' in partition
        assert 'oldest_available_offset' in partition
        assert 'newest_available_offset' in partition
