import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from orthanc_tools.orthanc_folder_importer import OrthancFolderImporter


class TestOrthancFolderImporter(unittest.TestCase):
    def test_empty_upload_result_is_not_retried(self):
        api_client = mock.Mock()
        api_client.upload.return_value = []
        api_client.is_alive.return_value = True

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "invalid.dcm")
            input_path.write_bytes(b"not dicom")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=str(Path(temp_dir, "errors.txt")),
                state_path=None,
                max_retries=1,
            )

            with mock.patch("orthanc_tools.orthanc_folder_importer.time.sleep"):
                importer.upload_and_label(str(input_path))

        api_client.upload.assert_called_once()
        api_client.is_alive.assert_called_once_with()
        api_client.instances.get_parent_study_id.assert_not_called()

    def test_zip_pdf_uses_the_study_uploaded_from_the_same_archive(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_path = Path(temp_dir, "study.zip")
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("report.pdf", b"pdf")
                archive.writestr("image.dcm", b"dicom")

            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(str(archive_path))

        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-1",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_pdf_import_rejects_multiple_studies_in_one_folder(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-1"], ["instance-2"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-1", "study-2"]

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "a.dcm").write_bytes(b"dicom-1")
            Path(temp_dir, "b.dcm").write_bytes(b"dicom-2")
            Path(temp_dir, "report.pdf").write_bytes(b"pdf")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            with self.assertRaisesRegex(RuntimeError, "multiple studies"):
                importer.upload_and_label(temp_dir)

        api_client.studies.attach_pdf.assert_not_called()

    def test_failed_pdf_import_is_not_checkpointed(self):
        api_client = mock.Mock()
        api_client.upload.return_value = []
        api_client.is_alive.return_value = True

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "invalid.dcm").write_bytes(b"not dicom")
            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=str(Path(temp_dir, "errors.txt")),
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            with self.assertRaisesRegex(RuntimeError, "Importer worker failed"):
                importer.execute()

            self.assertFalse(state_path.exists())

    def test_pdf_attachment_is_retried_before_checkpointing(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"
        api_client.studies.attach_pdf.side_effect = [RuntimeError("temporary"), None]

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "image.dcm").write_bytes(b"dicom")
            Path(temp_dir, "report.pdf").write_bytes(b"pdf")
            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=str(state_path),
                max_retries=1,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            with mock.patch("orthanc_tools.orthanc_folder_importer.time.sleep"):
                importer.execute()

            self.assertEqual([temp_dir], state_path.read_text().splitlines())

        self.assertEqual(2, api_client.studies.attach_pdf.call_count)

    def test_failed_pdf_does_not_checkpoint_a_nested_dicom_folder(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"
        api_client.studies.attach_pdf.side_effect = RuntimeError("failed")

        with tempfile.TemporaryDirectory() as temp_dir:
            image_path = Path(temp_dir, "images")
            image_path.mkdir()
            Path(image_path, "image.dcm").write_bytes(b"dicom")
            Path(temp_dir, "report.pdf").write_bytes(b"pdf")
            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            with self.assertRaisesRegex(RuntimeError, "Importer worker failed"):
                importer.execute()

            self.assertFalse(state_path.exists())

    def test_cli_help_builds_parser(self):
        result = subprocess.run(
            [sys.executable, "-m", "orthanc_tools.orthanc_folder_importer", "--help"],
            capture_output=True,
            text=True,
        )

        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
