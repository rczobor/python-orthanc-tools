import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from orthanc_tools.helpers.environment import get_env_bool
from orthanc_tools.orthanc_uploader import OrthancUploader
from orthanc_tools.orthanc_warmer import OrthancWarmer


class TestBooleanEnvironmentVariables(unittest.TestCase):
    def test_false_string_is_false(self):
        with mock.patch.dict(os.environ, {"FEATURE_ENABLED": "false"}):
            self.assertFalse(get_env_bool("FEATURE_ENABLED", True))

    def test_true_values_are_case_insensitive(self):
        with mock.patch.dict(os.environ, {"FEATURE_ENABLED": "YeS"}):
            self.assertTrue(get_env_bool("FEATURE_ENABLED"))

    def test_missing_value_uses_default(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(get_env_bool("FEATURE_ENABLED", True))

    def test_invalid_value_is_rejected(self):
        with mock.patch.dict(os.environ, {"FEATURE_ENABLED": "sometimes"}):
            with self.assertRaisesRegex(ValueError, "FEATURE_ENABLED"):
                get_env_bool("FEATURE_ENABLED")


class TestInjectedApiClients(unittest.TestCase):
    def test_uploader_uses_injected_client(self):
        api_client = mock.MagicMock()
        api_client.upload_file.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "instance.dcm").touch()
            uploader = OrthancUploader(api_client=api_client, path=temp_dir)

            uploader.upload_folder_and_label(temp_dir, ["reviewed"])

        api_client.upload_file.assert_called_once()
        api_client.instances.get_parent_study_id.assert_called_once_with("instance-id")
        api_client.studies.add_labels.assert_called_once_with(
            orthanc_id="study-id",
            labels=["reviewed"],
        )

    def test_warmer_uses_injected_client(self):
        api_client = mock.MagicMock()
        warmer = OrthancWarmer(api_client=api_client, interval=30)

        warmer.find()

        api_client.studies.find.assert_called_once_with(
            query={"StudyDate": "19500101"}
        )
        self.assertEqual(0, warmer._errors_counter)


if __name__ == "__main__":
    unittest.main()
