import phonebook
import unittest


class OrgChartTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_object_has_json(self):
        chart_object = phonebook.OrgChart()
        chart_object._boto_session = 'fake'
        chart_object._from_file = True
        hris_json = chart_object._load_file_from_s3()

        assert hris_json.get('Report_Entry', None) is not None
        people = hris_json.get('Report_Entry', None)
        assert len(people) > 0

    def test_object_filter(self):
        chart_object = phonebook.OrgChart()
        chart_object._boto_session = 'fake'
        chart_object._from_file = True
        hris_json = chart_object._load_file_from_s3()

        filtered = chart_object.filter_org_chart_attributes()
        whitelist = [
            'EmployeeID',
            'IsManager',
            'isDirectorOrAbove',
            'businessTitle',
            'PreferredFirstName',
            'Preferred_Name_-_Last_Name',
            'PrimaryWorkEmail',
            'WorkersManager',
            'WorkersManagersEmployeeID'
        ]

        assert len(filtered.get('Report_Entry')) > 0

        for record in filtered.get('Report_Entry'):
            for k, v in record.items():
                print(k)
                print(v)
                assert k in whitelist
