import unittest
import hack

class TestDummy(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_1_plus_1(self):
        assert 1 + 1 == 2

    def test_2_plus_2(self):
        assert 2 + 2 == 4

    def simple_app_test(self):
        client = hack.application.test_client()
        response = client.get('/topics')
        assert response.status_code == 401
