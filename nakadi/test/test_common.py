#!/usr/bin/env python3
import json

from nakadi import config

TEST_TOPIC = 'test-topic'
TEST_PARTITIONS_NUM = 8
config.ENABLE_LOGS_ROTATION = False

def get_monkey_patched_hack():
    # monkey patch to switch off authentication
    from nakadi import security
    def fake_authenticate(function):
        def function_wrapper(*args, **kwargs):
            return function(*args, **kwargs)
        return function_wrapper
    security.authenticate = fake_authenticate

    # monkey patch eventstore uid check
    from nakadi import hack
    def __fake_get_uid():
        return config.UID_TO_POST_EVENT
    hack.__get_uid = __fake_get_uid

    return hack


def validate_error_response(response, status_code, problem_detail):
    assert response.status_code == status_code
    problem = json.loads(response.data.decode('utf-8'))
    assert 'detail' in problem
    assert problem['detail'] == problem_detail


def create_dummy_event(partitioning_key):
    return {
        'event': 'dummy-type',
        'partitioning_key': partitioning_key,
        'meta_data': {
            'id': 'blah-id',
            'created': '11-11-1111'
        }
    }