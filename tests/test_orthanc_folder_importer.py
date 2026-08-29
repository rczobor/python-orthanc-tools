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

    def test_cli_help_builds_parser(self):
        result = subprocess.run(
            [sys.executable, "-m", "orthanc_tools.orthanc_folder_importer", "--help"],
            capture_output=True,
            text=True,
        )

        self.assertEqual(0, result.returncode, result.stderr)

    def test_zip_pdf_uses_uploaded_dicom_study(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_path = Path(temp_dir, "study.zip")
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("image.dcm", b"dicom")
                archive.writestr("report.pdf", b"pdf")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            returned_study_id = importer.upload_and_label(str(archive_path))

        self.assertEqual("study-id", returned_study_id)
        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-id",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_pdf_attachment_failure_is_retried_and_logged(self):
        api_client = mock.Mock()
        api_client.studies.attach_pdf.side_effect = RuntimeError("attach failed")

        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = Path(temp_dir, "report.pdf")
            errors_path = Path(temp_dir, "errors.txt")
            pdf_path.write_bytes(b"pdf")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=str(errors_path),
                state_path=None,
                max_retries=1,
                dicomize_pdf=True,
            )

            with mock.patch("orthanc_tools.orthanc_folder_importer.time.sleep"):
                importer.upload_and_label(str(pdf_path), study_orthanc_id="study-id")

            self.assertEqual(f"{pdf_path}\n", errors_path.read_text(encoding="utf-8"))

        self.assertEqual(2, api_client.studies.attach_pdf.call_count)

    def test_zip_preserves_study_across_sibling_directories(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_path = Path(temp_dir, "study.zip")
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("a-images/image.dcm", b"dicom")
                archive.writestr("b-reports/report.pdf", b"pdf")
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
            study_id="study-id",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_zip_processes_root_dicom_before_nested_report(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_path = Path(temp_dir, "study.zip")
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("reports/report.pdf", b"pdf")
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
            study_id="study-id",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_execute_preserves_study_across_sibling_directories(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            images_dir = Path(temp_dir, "a-images")
            reports_dir = Path(temp_dir, "b-reports")
            images_dir.mkdir()
            reports_dir.mkdir()
            Path(images_dir, "image.dcm").write_bytes(b"dicom")
            Path(reports_dir, "report.pdf").write_bytes(b"pdf")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-id",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_skipped_file_preserves_inherited_study(self):
        api_client = mock.Mock()

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "README.txt").write_text("notes", encoding="utf-8")
            Path(temp_dir, "report.pdf").write_bytes(b"pdf")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                skip_extensions=[".txt"],
                dicomize_pdf=True,
            )

            returned_study_id = importer.upload_and_label(
                temp_dir,
                study_orthanc_id="study-id",
            )

        self.assertEqual("study-id", returned_study_id)
        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-id",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )


if __name__ == "__main__":
    unittest.main()
