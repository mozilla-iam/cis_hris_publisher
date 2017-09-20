"""Search the Identity Vault by primary mozilla e-mail."""

from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from cis.settings import get_config


class Search(object):
    def __init__(self, boto_session):
        self.boto_session = boto_session
        self.dynamodb_table = None
        self.config = get_config()

    def _connect_dynamo_db(self):
        """New up a dynamodb resource from boto session."""
        dynamodb = self.boto_session.resource('dynamodb')
        dynamodb_table = self.config('dynamodb_table', namespace='cis')
        self.dynamodb_table = dynamodb.Table(dynamodb_table)

    def find_by_email(self, primary_email):
        """Searches the vault based on primary e-mail returns response result."""
        if not self.dynamodb_table:
            self._connect_dynamo_db()

        try:
            response = self.dynamodb_table.scan(
                Select='ALL_ATTRIBUTES',
                FilterExpression=Attr('primaryEmail').eq(primary_email)
            )

            if response.get('Count') > 0:
                return response['Items'][0]
        except ClientError as e:
            return None
