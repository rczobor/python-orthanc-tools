import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from orthanc_api_client import exceptions

from orthanc_tools.orthanc_cleaner import OrthancCleaner
from orthanc_tools.orthanc_files_checker import OrthancFilesChecker


class TestOrthancCleanerRules(unittest.TestCase):
    def test_optional_rule_columns_default_to_empty_strings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            rules_path = os.path.join(temp_dir, "labels.csv")
            with open(rules_path, "w") as rules_file:
                rules_file.write("LABEL1,6\nLABEL2,12,CT\nLABEL3,18,MR,ACC-3\n")

            cleaner = OrthancCleaner(
                api_client=mock.MagicMock(),
                execution_time=None,
                labels_file_path=rules_path,
            )

            rules = cleaner.parse_csv_file()

        self.assertEqual(
            [
                ("LABEL1", 6, "", ""),
                ("LABEL2", 12, "CT", ""),
                ("LABEL3", 18, "MR", "ACC-3"),
            ],
            [
                (
                    rule.label_name,
                    rule.retention_duration,
                    rule.modality,
                    rule.accession_number,
                )
                for rule in rules
            ],
        )


class TestOrthancFilesCheckerFailures(unittest.TestCase):
    def test_connection_failure_is_not_reported_as_missing_storage(self):
        api_client = mock.MagicMock()
        api_client.studies.get_all_ids.return_value = ["study-1"]
        api_client.studies.get_instances_ids.return_value = ["instance-1"]
        api_client.instances.get_file.side_effect = exceptions.ConnectionError()

        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = os.path.join(temp_dir, "missing.csv")
            checker = OrthancFilesChecker(api_client, report_path)

            with self.assertRaises(exceptions.ConnectionError):
                checker.check()

            self.assertFalse(os.path.exists(report_path))

    def test_missing_storage_file_is_reported(self):
        response = mock.Mock()
        response.json.return_value = {"OrthancStatus": 2006}
        missing_file_error = exceptions.HttpError(
            http_status_code=500,
            request_response=response,
        )
        study = SimpleNamespace(
            patient_main_dicom_tags={"PatientID": "PATIENT", "PatientName": "NAME"},
            main_dicom_tags={
                "StudyDate": "20260828",
                "StudyDescription": "DESCRIPTION",
                "StudyInstanceUID": "1.2.3",
            },
        )
        api_client = mock.MagicMock()
        api_client.studies.get_all_ids.return_value = ["study-1"]
        api_client.studies.get_instances_ids.return_value = ["instance-1"]
        api_client.instances.get_file.side_effect = missing_file_error
        api_client.studies.get.return_value = study

        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = os.path.join(temp_dir, "missing.csv")
            OrthancFilesChecker(api_client, report_path).check()

            with open(report_path) as report:
                self.assertEqual(
                    "PATIENT,NAME,20260828,DESCRIPTION,1.2.3\n",
                    report.read(),
                )


if __name__ == "__main__":
    unittest.main()
