"""Made for use on dev machines in dev environment only."""
import boto3
import unittest

from hris_publisher import vault


class HRISTest(unittest.TestCase):
    def setUp(self):
        v = vault.Search(boto_session=None)
        assert v is not None

    def test_search_by_email(self):
        boto_session = boto3.session.Session(region_name='us-west-2')
        v = vault.Search(boto_session)
        record = v.find_by_email('akrug@mozilla.com')
        assert record is not None
