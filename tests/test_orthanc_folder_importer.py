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

    def test_pdf_resume_reprocesses_checkpointed_child_for_study_context(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = Path(temp_dir, "input")
            images_dir = Path(input_dir, "a-images")
            reports_dir = Path(input_dir, "b-reports")
            input_dir.mkdir()
            images_dir.mkdir()
            reports_dir.mkdir()
            Path(images_dir, "image.dcm").write_bytes(b"dicom")
            Path(reports_dir, "report.pdf").write_bytes(b"pdf")
            state_path = Path(temp_dir, "state.txt")
            state_path.write_text(f"{images_dir}\n", encoding="utf-8")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=str(input_dir),
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

        api_client.upload.assert_called_once()
        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-id",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_execute_queues_top_level_entries_when_pdf_conversion_is_off(self):
        api_client = mock.Mock()

        with tempfile.TemporaryDirectory() as temp_dir:
            first_path = Path(temp_dir, "first.dcm")
            second_path = Path(temp_dir, "second.dcm")
            first_path.write_bytes(b"dicom")
            second_path.write_bytes(b"dicom")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                worker_threads_count=2,
            )

            with mock.patch.object(importer, "upload_and_label") as upload:
                importer.execute()

        self.assertCountEqual(
            [mock.call(path_to_upload=str(first_path)), mock.call(path_to_upload=str(second_path))],
            upload.call_args_list,
        )

    def test_upload_skips_parent_study_lookup_when_context_is_unused(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "image.dcm")
            input_path.write_bytes(b"dicom")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
            )

            importer.upload_and_label(str(input_path))

        api_client.instances.get_parent_study_id.assert_not_called()

    def test_failed_dicom_clears_previous_study_before_pdf(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-id"], []]
        api_client.instances.get_parent_study_id.return_value = "study-id"
        api_client.is_alive.return_value = True

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "a-image.dcm").write_bytes(b"dicom")
            Path(temp_dir, "b-invalid.dcm").write_bytes(b"invalid")
            Path(temp_dir, "c-report.pdf").write_bytes(b"pdf")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        api_client.studies.attach_pdf.assert_not_called()

    def test_pdf_only_zip_is_processed_after_dicom(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "image.dcm").write_bytes(b"dicom")
            archive_path = Path(temp_dir, "reports.zip")
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("report.pdf", b"pdf")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-id",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_path_root_matches_string_resume_checkpoint(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = Path(temp_dir, "input")
            input_dir.mkdir()
            Path(input_dir, "image.dcm").write_bytes(b"dicom")
            state_path = Path(temp_dir, "state.txt")
            state_path.write_text(f"{input_dir}\n", encoding="utf-8")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=input_dir,
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

        api_client.upload.assert_not_called()


if __name__ == "__main__":
    unittest.main()
