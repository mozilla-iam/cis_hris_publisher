"""Search the Identity Vault by primary mozilla e-mail."""
import boto3
from cis.settings import get_config


class CISTable(object):
    def __init__(self, table_name):
        self.boto_session = boto3.session.Session()
        self.table_name = table_name
        self.table = None

    def connect(self):
        resource = self.boto_session.resource('dynamodb')
        self.table = resource.Table(self.table_name)
        return self.table

    @property
    def all(self):
        if self.table is None:
            self.connect()
        return self.table.scan().get('Items')


class Search(object):
    def __init__(self, boto_session):
        self.boto_session = boto_session
        self.dynamodb_table = None
        self.config = get_config()
        self.people = CISTable(self.config('dynamodb_table', namespace='cis')).all

    def find_by_email(self, primary_email):
        """Searches the vault based on primary e-mail returns response result."""
        for person in self.people:
            if primary_email == person.get('primaryEmail'):
                return person
        return None
