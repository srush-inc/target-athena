import unittest
from nose.tools import assert_raises

import target_athena

class TestUnit(unittest.TestCase):
    """
    Unit Tests
    """
    @classmethod
    def setUp(self):
        self.config = {}


    def test_naming_convention_replaces_tokens(self):
        """Test that the naming_convention tokens are replaced"""
        stream_name = 'the_stream'
        message = {
            'stream': stream_name
        }
        timestamp = 'fake_timestamp'
        s3_key = target_athena.utils.get_target_key(stream_name, timestamp=timestamp, naming_convention='test_{stream}_{timestamp}_test.csv')

        self.assertEqual('test_the_stream_fake_timestamp_test.csv', s3_key)


    def test_naming_convention_has_reasonable_default(self):
        """Test the default value of the naming convention"""
        stream_name = 'the_stream'
        message = {
            'stream': stream_name
        }
        s3_key = target_athena.utils.get_target_key(stream_name)

        # default is "{stream}-{timestamp}.csv"
        self.assertTrue(s3_key.startswith('the_stream'))
        self.assertTrue(s3_key.endswith('.csv'))


    def test_naming_convention_honors_prefix(self):
        """Test that if the prefix is set in the config, that it is used in the s3 key"""
        stream_name = 'the_stream'
        message = {
            'stream': stream_name
        }
        s3_key = target_athena.utils.get_target_key(stream_name, prefix='the_prefix__', naming_convention='folder1/test_{stream}_test.csv')

        self.assertEqual('folder1/the_prefix__test_the_stream_test.csv', s3_key)
