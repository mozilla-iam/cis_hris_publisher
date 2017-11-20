import base64
import boto3
import json
import logging

import hris
import task
import vault

from cis.libs import utils
from cis.settings import get_config


sl = utils.StructuredLogger(name=__name__, level=logging.DEBUG)
# Logger setUp
logger = logging.getLogger(__name__)

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

def handle(event=None, context={}):
    # Load the file of HRIS Data.
    boto_session = boto3.session.Session(region_name='us-west-2')
    hris_json = hris.HrisJSON(boto_session)
    hr_data = hris_json.load()

    v = vault.Search(boto_session)

    valid_records = []
    dead_letters = []
    invalid_records = []

    cis_publisher_session = assume_role_session()

    # For each user validate they have the required fields.
    for record in hr_data.get('Report_Entry'):
        # If user valid go ahead and push them onto a list to use as a stack.
        email = record.get('PrimaryWorkEmail')

        if hris_json.is_valid(record):
            valid_records.append(record)
            hris_groups = hris_json.to_groups(record)
        else:
            logger.error('Record invalid for : {user} deadlettering workday record.'.format(user=email))
            dead_letters.append(record)

    # For each user in the valid list
    for record in valid_records:
        logger.info('Processing record {}'.format(record))
        email = record.get('PrimaryWorkEmail')
        # Retrieve their current profile from the identity vault.
        vault_record = v.find_by_email(email)

        # Enrich the profile with the new data fields from HRIS extract. (Groups only for now)
        if vault_record is not None:
            t = task.CISTask(
                boto_session=cis_publisher_session,
                vault_record=vault_record,
                hris_groups=hris_groups
            )

            data = t.prep()
            t.send(data)
        else:
            logger.error('Could not find record in vault for user: {user}'.format(user=email))
            invalid_records.append('1')
        continue

    logger.info('Processing complete dead_letter_count: {dl}, valid_records: {vr}, unlocated_in_vault: {iv}'.format(
            dl=len(dead_letters),
            vr=len(valid_records),
            iv=len(invalid_records)
        )
    )
