import base64

import json
import logging

import pprint

from cis.publisher import ChangeDelegate
from cis.settings import get_config


logger = logging.getLogger(__name__)


class CISTask(object):
    def __init__(self, boto_session, vault_record, hris_groups):
        self.boto_session = boto_session
        self.vault_record = vault_record
        self.hris_groups = hris_groups

    @property
    def publisher(self):
        config = get_config()
        return {
            'id': config('publisher_name', namespace='cis')
        }

    def prep(self):
        current_group_list = self.get_groups_for_record()
        current_group_list = self.clean_hris_assertions(current_group_list)
        self.reintegrate_groups(current_group_list)
        return self.construct_profile()

    def send(self, data):
        cis_change = ChangeDelegate(self.publisher, {}, data)
        cis_change.boto_session = self.boto_session


        event = {
            'profile': base64.b64encode(cis_change._prepare_profile_data()).decode(),
            'publisher': {'id': 'hris'},
            'signature': {}
        }

        encrypted_profile_data = json.loads(base64.b64decode(event.get('profile')))

        for key in ['ciphertext', 'ciphertext_key', 'iv', 'tag']:
            encrypted_profile_data[key] = base64.b64decode(encrypted_profile_data[key])

        result = cis_change._invoke_validator(json.dumps(event))

        logger.info('Result of the change for user: {user} is {result}'.format(
                user=self.vault_record.get('primaryEmail'),
                result=result
            )
        )

        return result

    def get_groups_for_record(self):
        return self.vault_record.get('groups')

    def clean_hris_assertions(self, current_group_list):
        for group in current_group_list:
            # Remove existing groups with hris_ attrs from assertions.
            if group.find('hris_') == 0:
                logger.info('Removing current hris_ assertion from {user} for group : {group}'.format(
                        user=self.vault_record.get('primaryEmail'),
                        group=group
                    )
                )
                current_group_list.pop(current_group_list.index(group))
        return current_group_list

    def reintegrate_groups(self, current_group_list):
        # Add the new assumptions we made about the user to the current grouplist
        self.vault_record['groups'] = current_group_list + self.hris_groups
        logger.info('Groups reintegrated for user: {user}'.format(user=self.vault_record.get('primaryEmail')))

    def construct_profile(self):
        logger.info('Sending profile to CIS for: {user}'.format(user=self.vault_record.get('primaryEmail')))
        data = self.vault_record
        return data
