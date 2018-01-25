import base64

import json
import logging


from cis.publisher import ChangeDelegate
from cis.settings import get_config


logger = logging.getLogger('cis_hris')


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

    def send(self, data):
        cis_change = ChangeDelegate(self.publisher, {}, data)
        cis_change.boto_session = self.boto_session

        vault_record['groups'] = hris_groups

        event = {
            'profile': base64.b64encode(cis_change._prepare_profile_data()).decode(),
            'publisher': {'id': 'hris'},
            'signature': None
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
