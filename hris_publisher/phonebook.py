"""Class to support output to phonebook format."""
import boto3
import json
import hris
import logging

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class OrgChart(object):
    def __init__(self):
        self._hris_json = None
        self._boto_session = None
        self._from_file = False

    @property
    def attr_whitelist(self):
        return [
            'EmployeeID',
            'IsManager',
            'isDirectorOrAbove',
            'businessTitle',
            'PreferredFirstName',
            'Preferred_Name_-_Last_Name',
            'PrimaryWorkEmail',
            'WorkersManager',
            'WorkersManagersEmployeeID'
        ]

    @property
    def ceo_whitelist(self):
        return [
            'EmployeeID',
            'IsManager',
            'isDirectorOrAbove',
            'businessTitle',
            'PreferredFirstName',
            'Preferred_Name_-_Last_Name',
            'PrimaryWorkEmail',
        ]

    def _connect(self):
        self._boto_session = boto3.session.Session(
            region_name='us-west-2'
        )
        return self._boto_session

    def _load_file_from_s3(self):
        if self._boto_session is None:
            self._connect()

        if self._hris_json is None:
            hris_json_file = hris.HrisJSON(self._boto_session)
            hris_json_file.from_file = self._from_file
            self._hris_json = hris_json_file.load()
        return self._hris_json

    def filter_org_chart_attributes(self):
        filtered_hris_json = {
            'Report_Entry': []
        }
        for record in self._hris_json.get('Report_Entry'):
            filtered_record = {}
            if record.get('businessTitle') == 'CEO':
                for k, v in record.items():
                    if k in self.ceo_whitelist and v is not None:
                        filtered_record[k] = v
                filtered_hris_json['Report_Entry'].append(filtered_record)
                continue
            for k, v in record.items():
                if k in self.attr_whitelist and v is not None:
                    filtered_record[k] = v
            filtered_hris_json['Report_Entry'].append(filtered_record)
        return filtered_hris_json

    def to_s3(self, filtered_hris_json):
        if self._boto_session is None:
            self._connect()

        self._locate_s3_bucket()
        client = self._boto_session.client('s3', region_name='us-west-2')
        s3 = self._boto_session.resource('s3')

        object = s3.Object(
            self.bucket_name,
            'org_chart.json'
        )

        return object.put(Body=json.dumps(filtered_hris_json).encode())

    def _locate_s3_bucket(self):
        """Find the S3 bucket in the account using bucket tags."""
        if self._boto_session is None:
            self._boto_session = self._connect()

        client = self._boto_session.client('s3', region_name='us-west-2')
        s3 = self._boto_session.resource('s3')
        for this_bucket in client.list_buckets().get('Buckets'):
            bucket_name = this_bucket.get('Name')
            bucket_tagging = s3.BucketTagging(bucket_name)

            try:
                tag_set = bucket_tagging.tag_set
                if self._is_tag_match(tag_set):
                    self.bucket_name = bucket_name
                    return bucket_name
            except ClientError:
                # Boto3 throws client error when a bucket has no tags.
                logger.error('No bucket could be located in the account with HRIS data.  Exiting.')
                continue

    def _is_tag_match(self, tag_set):
        for tag in tag_set:
            logger.info('Attemption to match tag: {}'.format(tag))
            if tag.get('Value') == 'hris_publisher':
                logger.info('Tag matched. Returning.')
                return True
        return False
