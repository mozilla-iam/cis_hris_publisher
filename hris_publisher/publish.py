import base64
import boto3
import json
import logging
import hris
import vault

from cis.libs import utils
from cis.publisher import ChangeDelegate
from cis.settings import get_config

import pprint

logger = logging.getLogger(__name__)
sl = utils.StructuredLogger(name=__name__, level=logging.DEBUG)


def dead_letter():
    """Send the profile to the dead letter queue for manual inspection."""
    # XXX TBD
    pass


def handle(event=None, context={}):
    # Load config
    config = get_config()

    # Load the file of HRIS Data.
    boto_session = boto3.session.Session(region_name='us-west-2')

    hris_json = hris.HrisJSON(boto_session)

    hr_data = hris_json.load()

    v = vault.Search(boto_session)

    valid_records = []
    dead_letters = []
    invalid_records = []
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
        email = record.get('PrimaryWorkEmail')
        # Retrieve their current profile from the identity vault.
        vault_record = v.find_by_email(email)

        # Enrich the profile with the new data fields from HRIS extract. (Groups only for now)
        if vault_record is not None:
            logger.info('User record located in identity vault for: {user}'.format(user=email))
            current_group_list = vault_record.get('groups')

            for group in current_group_list:
                # Remove existing groups with hris_ attrs from assertions.
                if group.find('hris_') == 0:
                    logger.info('Removing current hris_ assertion from {user} for group : group'.format(
                            user=email,
                            group=group
                        )
                    )
                    current_group_list.pop(group)

            # Add the new assumptions we made about the user to the current grouplist
            vault_record['groups'] = current_group_list + hris_groups
            logger.info('Groups reintegrated for user: {user}'.format(user=email))
            # Add a signature XXX TBD

            # Send to CIS Validator Function
            logger.info('Sending profile to CIS for: {user}'.format(user=email))

            sts = boto3.client('sts')
            sts_response = sts.assume_role(
                RoleArn=config('iam_role_arn', namespace='cis'),
                RoleSessionName=config('iam_role_session_name', namespace='cis')
            )
            logger.info('CIS Publisher role assumed.')

            cis_publisher_session = boto3.session.Session(
                aws_access_key_id=sts_response['Credentials']['AccessKeyId'],
                aws_secret_access_key=sts_response['Credentials']['SecretAccessKey'],
                aws_session_token=sts_response['Credentials']['SessionToken'],
                region_name='us-west-2'
            )

            publisher = {
                'id': config('publisher_name', namespace='cis')
            }

            data = {
                'user_id': vault_record.get('user_id'),
                'timezone':  vault_record.get('timezone'),
                'active': vault_record.get('active'),
                'lastModified': vault_record.get('lastModified'),
                'created': vault_record.get('created'),
                'userName': vault_record.get('userName'),
                'displayName': vault_record.get('displayName'),
                'primaryEmail': vault_record.get('primaryEmail'),
                'emails': vault_record.get('emails'),
                'uris': vault_record.get('uris'),
                'picture': vault_record.get('picture'),
                'shirtSize': vault_record.get('shirtSize'),
                'groups':vault_record.get('groups'),
                'firstName': vault_record.get('firstName'),
                'lastName': vault_record.get('lastName'),

                # XXX TBD Fix this once the attributes are populated and can search person API.
                # Hardcoded fields these can not currently be set in profile editor.
                # Future integration for Mozillians.org
                'preferredLanguage': 'en_US',
                'phoneNumbers': [],
                'nicknames': [],
                'SSHFingerprints': [],
                'PGPFingerprints': [],
                'authoritativeGroups': []
            }

            cis_change = ChangeDelegate(publisher, {}, data)
            cis_change.boto_session = cis_publisher_session

            result = cis_change.send()
            logger.info('Result of the change for user: {user} is {result}'.format(
                    user=email,
                    result=result
                )
            )
        else:
            # logger.error('Could not find record in vault for user: {user}'.format(user=email))
            invalid_records.append('1')
            continue

    logger.info('Processing complete dead_letter_count: {dl}, valid_records: {vr}, unlocated_in_vault: {iv}'.format(
            dl=len(dead_letters),
            vr=len(valid_records),
            iv=len(invalid_records)
        )
    )
