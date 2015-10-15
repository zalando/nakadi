#!/usr/bin/env python3
import json
import unittest
import security

# monkey patch to switch off authentication
def fake_authenticate(function):
    def function_wrapper(*args, **kwargs):
        return function(*args, **kwargs)
    return function_wrapper
security.authenticate = fake_authenticate

import hack


# monkey patch eventstore uid check
def __fake_uid_check():
    return True
hack.__uid_is_valid_to_post = __fake_uid_check

# test expects the following configuration in kafka
TEST_TOPIC = 'test-topic'
TEST_PARTITIONS_NUM = 128


# this test case doesn't care if there are any events present or not
# for api version 0.3
class EventstoreDataIndependentTestCase(unittest.TestCase):


    def setUp(self):
        self.app = hack.application.test_client()


    def tearDown(self):
        pass


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
        self.__validate_error_response(response, 404, 'topic not found')


    def test_when_get_not_existing_partition_then_partition_not_found(self):
        response = self.app.get('/topics/%s/partitions/2341' % TEST_TOPIC)
        self.__validate_error_response(response, 404, 'partition not found')


    def test_when_get_letter_partition_then_partition_not_a_number(self):
        response = self.app.get('/topics/%s/partitions/ab' % TEST_TOPIC)
        self.__validate_error_response(response, 400, '"partition" path parameter should be an integer number')


    def test_when_get_partitions_for_not_existing_topic_then_topic_not_found(self):
        response = self.app.get('/topics/not_existing_topic/partitions')
        self.__validate_error_response(response, 404, 'topic not found')


    def test_when_post_event_then_ok(self):
        response = self.app.post('/topics/%s/events' % TEST_TOPIC,
                                 headers = {'Content-type': 'application/json'},
                                 data = json.dumps(self.__create_dummy_event()))
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


    def __create_dummy_event(self):
        return {
            'event_type': 'dummy-type',
            'ordering_key': 'dummy-key',
            'metadata': {
                'dummy-field': 'blah'
            }
        }


    def __validate_error_response(self, response, status_code, problem_detail):
        assert response.status_code == status_code
        problem = json.loads(response.data.decode('utf-8'))
        assert 'detail' in problem
        assert problem['detail'] == problem_detail


    def __validate_partition_structure(self, partition):
        assert 'partition_id' in partition
        assert 'oldest_available_offset' in partition
        assert 'newest_available_offset' in partition


if __name__ == '__main__':
    unittest.main()