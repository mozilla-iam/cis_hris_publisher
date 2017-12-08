import boto3
import json
import logging
import os

from cis.libs import utils
from botocore.exceptions import ClientError
from jsonschema import validate as jsonschema_validate
from jsonschema.exceptions import ValidationError


sl = utils.StructuredLogger(name=__name__, level=logging.DEBUG)
logger = logging.getLogger(__name__)


class HrisJSON(object):
    """Retrieval and serialization object for the current HRIS data file."""
    def __init__(self, boto_session):
        self.boto_session = boto_session
        self.from_file = False
        self.file_name = 'workday.json'
        self.s3_bucket = None
        self.bucket_name = None

    def load(self):
        """Fetch the file as stream from the s3 bucket."""
        if self.from_file is True:
            # From file set only for development purposes.
            hris_json = os.path.join(
                os.path.abspath(os.path.dirname(__file__)),
                '../sample_data/{file_name}'.format(file_name=self.file_name)
            )

            with open(hris_json, 'r') as schema_data:
                hris_json = json.load(schema_data)
        else:
            # Go get it from s3 using the helper functions
            self._locate_s3_bucket()
            streaming_body = self._get_object_from_bucket()
            hris_json = self._read_object(streaming_body)
        return hris_json

    def is_valid(self, entry):
        """
        Validate the object against the HRIS schema json file included in the project.
        :param entry: a single json entry from the hris.json
        :return: bool (truthy)
        """
        try:
            hris_schema_json = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'schema.json')

            with open(hris_schema_json, 'r') as schema_data:
                hris_schema_json = json.load(schema_data)

            jsonschema_validate(entry, hris_schema_json)
            return True
        except ValidationError:
            return False

    def to_groups(self, entry):
        """
        :param entry: from the hris.json
        :return: a list of groups to reintegrate to cis beginning with hris_
        """
        g = Groups(entry=entry)
        return g.all

    def _locate_s3_bucket(self):
        """Fine the S3 bucket in the account using bucket tags."""
        if self.boto_session is None:
            self.boto_session = self._connect_boto_session()

        client = self.boto_session.client('s3', region_name='us-west-2')
        s3 = self.boto_session.resource('s3')
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

    def _connect_boto_session(self):
        return boto3.session.Session()

    def _get_object_from_bucket(self):
        """Get the byte stream of the hris.json file."""
        if self.boto_session is None:
            self.boto_session = self._connect_boto_session()

        client = self.boto_session.client('s3', region_name='us-west-2')
        response = client.get_object(
            Bucket=self.bucket_name,
            Key=self.file_name
        )
        return response.get('Body')

    def _read_object(self, streaming_body):
        """Return the content of the file object byte stream we retrieved."""

        return json.loads(streaming_body.read())


class Groups(object):
    """Return a list of groups for a given profile based on rules."""
    def __init__(self, entry):
        self.hris_entry = entry
        self.hris_grouplist = []

    @property
    def all(self):
        """Run all rules and return the grouplist from the constructor."""
        self.cost_center_rule()
        self.cost_center_hierarchy()
        self.management_level_rule()
        self.manager_name_rule()
        self.manager_status_rule()
        self.egencia_country_rule()
        self.is_staff_rule()

        return self.hris_grouplist

    @property
    def active(self):
        """Helper function used in profile creation."""
        if self.is_staff_rule is not None:
            return True
        else:
            return False

    def is_staff_rule(self):
        """Check if CurrentlyActive is 1 and add to group accordingly."""
        if self.hris_entry.get('CurrentlyActive') == "1":
            group_name = 'hris_is_staff'
            return self._add_group(group_name)

    def cost_center_rule(self):
        """Assert group based on splatting cost center."""
        # Will be number only

        full_cost_center = self.hris_entry.get('Cost_Center')
        cost_center_code = full_cost_center.split(' ')[0]

        cost_center_code = int(cost_center_code)

        group_name = 'hris_costcenter_{id}'.format(id=cost_center_code)
        return self._add_group(group_name)

    def cost_center_hierarchy(self):
        """Dept groups (really)"""
        dept = self._replace_spaces(self.hris_entry.get('Cost_Center_Hierarchy'))
        group_name = 'hris_dept_{dept}'.format(dept=dept)
        return self._add_group(group_name)

    def management_level_rule(self):
        """Assert group based on management level."""
        management_level = self.hris_entry.get('Management_Level')

        group_name = 'hris_{mangement_level}'.format(mangement_level=self._replace_spaces(management_level))
        return self._add_group(group_name)

    def manager_name_rule(self):
        """Add to group for direct manager."""
        manager_name = self.hris_entry.get('WorkersManager')
        manager_name = self._replace_spaces(manager_name)

        group_name = 'hris_direct_reports_{manager_name}'.format(manager_name=manager_name)

        return self._add_group(group_name)

    def manager_status_rule(self):
        """Add to group is isManager"""
        managerStatus = self.hris_entry.get('IsManager')
        if managerStatus == 'TRUE':
            group_name = 'hris_managers'
        else:
            group_name = 'hris_nonmanagers'
        return self._add_group(group_name)

    def egencia_country_rule(self):
        country = self.hris_entry.get('EgenciaPOSCountry')
        group_name = 'hris_egencia_{country}'.format(country=country)
        return self._add_group(group_name)

    def _add_group(self, group_name):
        group_name = self._to_lower(group_name)

        if group_name is not None:
            self.hris_grouplist.append(group_name)
            return group_name
        else:
            return None

    def _replace_spaces(self, input_string):
        if input_string:
            return input_string.replace(" ", "_")

    def _to_lower(self, input_string):
        return input_string.lower()
