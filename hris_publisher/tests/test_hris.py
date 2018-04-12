import hris
import unittest


class HRISTest(unittest.TestCase):
    def setUp(self):
        h = hris.HrisJSON(boto_session=None)
        h.file_name = 'hris.json'
        assert h is not None

    def test_file_loads(self):
        h = hris.HrisJSON(boto_session=None)
        h.file_name = 'hris.json'
        h.from_file = True
        hris_mock_json = h.load()
        assert isinstance(hris_mock_json, dict)

    def test_group_object(self):
        h = hris.HrisJSON(boto_session=None)
        h.file_name = 'hris.json'
        h.from_file = True
        hris_mock_json = h.load()

        hris_mock_json_entry = hris_mock_json.get('Report_Entry')[0]

        g = hris.Groups(
            entry=hris_mock_json_entry
        )

        assert g is not None

    def test_group_object_returns_list(self):
        h = hris.HrisJSON(boto_session=None)
        h.file_name = 'hris.json'
        h.from_file = True
        hris_mock_json = h.load()

        hris_mock_json_entry = hris_mock_json.get('Report_Entry')[0]

        g = hris.Groups(
            entry=hris_mock_json_entry
        )

        group_list = g.all

        assert isinstance(group_list, list)

    def test_group_object_rules(self):
        h = hris.HrisJSON(boto_session=None)
        h.file_name = 'hris.json'
        h.from_file = True
        hris_mock_json = h.load()

        hris_mock_json_entry = hris_mock_json.get('Report_Entry')[0]

        g = hris.Groups(
            entry=hris_mock_json_entry
        )

        group_list = g.all

        cost_center_group_name = g.cost_center_rule()
        cost_center_hierarchy = g.cost_center_hierarchy()
        management_status_rule = g.manager_status_rule()
        manager_name_rule = g.manager_name_rule()
        management_level_rule = g.management_level_rule()

        assert g.hris_entry == hris_mock_json_entry
        assert isinstance(group_list, list)
        assert cost_center_group_name == 'hris_costcenter_9999'
        assert cost_center_hierarchy == 'hris_dept_north'
        assert management_status_rule == 'hris_nonmanagers'
        assert manager_name_rule == 'hris_direct_reports_unknown'
        assert management_level_rule == 'hris_individual_contributor'

        groups_present = [
            'hris_costcenter_9999',
            'hris_dept_north',
            'hris_individual_contributor',
            'hris_direct_reports_unknown',
            'hris_nonmanagers',
            'hris_egencia_ws'
        ]

        for group in groups_present:
            assert group in g.hris_grouplist

    def test_groups_returned_from_hris_json(self):
        groups_present = [
            'hris_costcenter_9999',
            'hris_dept_north',
            'hris_individual_contributor',
            'hris_direct_reports_unknown',
            'hris_nonmanagers',
            'hris_egencia_ws',
            'hris_is_staff'
        ]

        h = hris.HrisJSON(boto_session=None)
        h.from_file = True
        h.file_name = 'hris.json'
        hris_mock_json = h.load()

        hris_mock_json_entry = hris_mock_json.get('Report_Entry')[0]

        groups = h.to_groups(hris_mock_json_entry)

        assert 'hris_dept_north' in groups
        assert 'hris_individual_contributor' in groups
        assert 'hris_nonmanagers' in groups
        assert 'hris_egencia_ws' in groups
        assert 'hris_is_staff' in groups
        assert 'hris_workertype_employee' in groups

    def test_groups_validator(self):
        h = hris.HrisJSON(boto_session=None)
        h.from_file = True
        h.file_name = 'hris-bad.json'
        hris_mock_json = h.load()
        hris_mock_json_entry = hris_mock_json.get('Report_Entry')[0]

        bad_status = h.is_valid(hris_mock_json_entry)

        h.file_name = 'hris.json'
        hris_mock_json = h.load()
        hris_mock_json_entry = hris_mock_json.get('Report_Entry')[0]

        good_status = h.is_valid(hris_mock_json_entry)

        assert bad_status is False
        assert good_status is True
