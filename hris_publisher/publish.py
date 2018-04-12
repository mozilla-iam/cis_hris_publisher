import boto3
import logging
import os

import hris
import task
import phonebook
import threading

from cis.libs import utils
from cis.libs.api import Person
from cis.settings import get_config
from utils import get_secret


sl = utils.StructuredLogger(name='cis_hris', level=logging.DEBUG)
# Logger setUp
logger = logging.getLogger('cis_hris')

def dead_letter():
    """Send the profile to the dead letter queue for manual inspection."""
    # XXX TBD
    pass

def assume_role_session():
    # Load config
    config = get_config()
    sts = boto3.client('sts')
    sts_response = sts.assume_role(
        RoleArn=config('iam_role_arn', namespace='cis'),
        RoleSessionName=config('iam_role_session_name', namespace='cis')
    )
    logger.info('CIS Publisher role assumed.')

    return boto3.session.Session(
        aws_access_key_id=sts_response['Credentials']['AccessKeyId'],
        aws_secret_access_key=sts_response['Credentials']['SecretAccessKey'],
        aws_session_token=sts_response['Credentials']['SessionToken'],
        region_name='us-west-2'
    )

def publish(record, boto_session, cis_publisher_session, hris_json, person_api):
    # If user valid go ahead and push them onto a list to use as a stack.
    email = record.get('PrimaryWorkEmail')

    if hris_json.is_valid(record):
        email = record.get('PrimaryWorkEmail', None)

        # Retrieve their current profile from the identity vault using person-api.
        logger.info('Attempting retrieval of user: {}'.format(email))
        if email is not None:
            vault_record = person_api.get_userinfo('ad|{}|{}'.format(os.getenv('LDAP_NAMESPACE'), email.split('@')[0]))
        else:
            vault_record = None

        # Enrich the profile with the new data fields from HRIS extract. (Groups only for now)
        if vault_record is not None and vault_record != {}:
            logger.info('Processing record :{}'.format(record))
            hris_groups = hris_json.to_groups(record)

            t = task.CISTask(
                boto_session=cis_publisher_session,
                vault_record=vault_record,
                hris_groups=hris_groups
            )

            res = t.send()
            logger.info('Data sent to identity vault for: {}'.format(email))
            logger.info('Result of operation in identity vault: {}'.format(res))
        else:
            logger.error('Could not find record in vault for user: {user}'.format(user=email))


def handle(event=None, context={}):
    boto_session = boto3.session.Session(region_name='us-west-2')
    hris_json = hris.HrisJSON(boto_session)
    hr_data = hris_json.load()

    boto_session = boto3.session.Session(region_name='us-west-2')
    cis_publisher_session = assume_role_session()
    hris_json = hris.HrisJSON(boto_session)

    # Load the file of HRIS Data.
    os.environ["CIS_OAUTH2_CLIENT_ID"] = get_secret('cis_hris_publisher.client_id', dict(app='cis_hris_publisher'))
    os.environ["CIS_OAUTH2_CLIENT_SECRET"] = get_secret('cis_hris_publisher.client_secret', dict(app='cis_hris_publisher'))

    person_api = Person(
        person_api_config = {
            'audience': os.getenv('CIS_PERSON_API_AUDIENCE'),
            'client_id': os.getenv('CIS_OAUTH2_CLIENT_ID'),
            'client_secret': os.getenv('CIS_OAUTH2_CLIENT_SECRET'),
            'oauth2_domain': os.getenv('CIS_OAUTH2_DOMAIN'),
            'person_api_url': os.getenv('CIS_PERSON_API_URL'),
            'person_api_version': os.getenv('CIS_PERSON_API_VERSION')
        }
    )

    threads = []

    for record in hr_data.get('Report_Entry'):
        t = threading.Thread(target=publish, args=[record, boto_session, cis_publisher_session, hris_json, person_api])
        threads.append(t)
        t.start()

    for thread in threads:
        thread.join()

def orgchart(event=None, context={}):
    p = phonebook.OrgChart()
    p._load_file_from_s3()
    filtered_attributes = p.filter_org_chart_attributes()
    p.to_s3(filtered_attributes)
