import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call

from orthanc_tools.hl7Lib.hl7_dicom_worklist_builder import DicomWorklistBuilder


class TestWorklistReplacement(unittest.TestCase):
    def setUp(self):
        self.worklists = Mock()
        self.worklists.get_all.return_value = [
            {"ID": "old-one", "Tags": {"AccessionNumber": "order-123"}},
            {"ID": "unrelated", "Tags": {"AccessionNumber": "other-order"}},
            {"ID": "old-two", "Tags": {"AccessionNumber": "order-123"}},
        ]
        self.worklists.create.return_value = "new-worklist"
        self.builder = DicomWorklistBuilder(
            orthanc_client=SimpleNamespace(worklists=self.worklists)
        )
        self.values = {
            "AccessionNumber": "order-123",
            "PatientID": "patient-123",
            "PatientName": "Example^Patient",
            "PatientBirthDate": "19700101",
            "PatientSex": "F",
            "RequestedProcedureID": "procedure-123",
            "SpecificCharacterSet": "ISO_IR 100",
            "ScheduledStationAETitle": "CT",
            "ScheduledProcedureStepID": "step-123",
        }

    def test_invalid_replacement_preserves_existing_worklists(self):
        del self.values["PatientID"]

        with self.assertRaisesRegex(Exception, "missing field 'PatientID'"):
            self.builder.generate(self.values, avoid_duplicates=True)

        self.assertEqual([], self.worklists.mock_calls)

    def test_failed_creation_does_not_delete_existing_worklists(self):
        self.worklists.create.side_effect = RuntimeError("creation failed")

        with self.assertRaisesRegex(RuntimeError, "creation failed"):
            self.builder.generate(self.values, avoid_duplicates=True)

        self.worklists.delete.assert_not_called()

    def test_successful_creation_precedes_removal_of_all_matching_items(self):
        result = self.builder.generate(self.values, avoid_duplicates=True)

        self.assertEqual("new-worklist", result)
        self.assertEqual([
            call.get_all(),
            call.create(values=self.worklists.create.call_args.kwargs["values"]),
            call.delete(orthanc_id="old-one"),
            call.delete(orthanc_id="old-two"),
        ], self.worklists.mock_calls)
        self.assertIn("StudyInstanceUID", self.worklists.create.call_args.kwargs["values"])

    def test_reused_new_id_is_not_deleted(self):
        self.worklists.create.return_value = "old-one"

        result = self.builder.generate(self.values, avoid_duplicates=True)

        self.assertEqual("old-one", result)
        self.worklists.delete.assert_called_once_with(orthanc_id="old-two")

    def test_invalid_creation_response_preserves_existing_worklists(self):
        self.worklists.create.return_value = None

        with self.assertRaisesRegex(ValueError, "did not return an Orthanc ID"):
            self.builder.generate(self.values, avoid_duplicates=True)

        self.worklists.delete.assert_not_called()

    def test_empty_accession_does_not_remove_unidentified_orders(self):
        self.values["AccessionNumber"] = ""
        self.worklists.get_all.return_value = [{"ID": "old", "Tags": {"AccessionNumber": ""}}]

        result = self.builder.generate(self.values, avoid_duplicates=True)

        self.assertEqual("new-worklist", result)
        self.worklists.get_all.assert_not_called()
        self.worklists.delete.assert_not_called()

    def test_delete_failure_leaves_the_created_replacement_available(self):
        self.worklists.delete.side_effect = RuntimeError("deletion failed")

        with self.assertRaisesRegex(RuntimeError, "deletion failed"):
            self.builder.generate(self.values, avoid_duplicates=True)

        self.worklists.create.assert_called_once()
        self.worklists.delete.assert_called_once_with(orthanc_id="old-one")
