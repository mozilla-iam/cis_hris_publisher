import logging

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

        result = cis_change.send()
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

            data = {
                'user_id': self.vault_record.get('user_id'),
                'timezone':  self.vault_record.get('timezone'),
                'active': self.vault_record.get('active'),
                'lastModified': self.vault_record.get('lastModified'),
                'created': self.vault_record.get('created'),
                'userName': self.vault_record.get('userName'),
                'displayName': self.vault_record.get('displayName'),
                'primaryEmail': self.vault_record.get('primaryEmail'),
                'emails': self.vault_record.get('emails'),
                'uris': self.vault_record.get('uris'),
                'picture': self.vault_record.get('picture'),
                'shirtSize': self.vault_record.get('shirtSize'),
                'groups':self.vault_record.get('groups'),
                'firstName': self.vault_record.get('firstName'),
                'lastName': self.vault_record.get('lastName'),

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

            return data
