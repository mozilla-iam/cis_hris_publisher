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

    def send(self):
        self.vault_record['groups'] = self.hris_groups
        cis_change = ChangeDelegate(self.publisher, {}, self.vault_record)
        cis_change.boto_session = self.boto_session

        result = cis_change.send()

        logger.info('Result of the change for user: {user} is {result}'.format(
                user=self.vault_record.get('primaryEmail'),
                result=result
            )
        )

        return result
