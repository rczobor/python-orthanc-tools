import os
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

    def test_flat_dicom_pdf_pairs_keep_their_study(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "study-a.dcm").write_bytes(b"dicom-a")
            Path(temp_dir, "study-a.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "study-b.dcm").write_bytes(b"dicom-b")
            Path(temp_dir, "study-b.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            [
                mock.call(
                    study_id="study-a",
                    pdf_path=str(Path(temp_dir, "study-a.pdf")),
                    series_description="PDF report",
                ),
                mock.call(
                    study_id="study-b",
                    pdf_path=str(Path(temp_dir, "study-b.pdf")),
                    series_description="PDF report",
                ),
            ],
            api_client.studies.attach_pdf.call_args_list,
        )

    def test_nested_pdf_only_zip_is_processed_after_dicom(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "image.dcm").write_bytes(b"dicom")
            reports_dir = Path(temp_dir, "reports")
            reports_dir.mkdir()
            with zipfile.ZipFile(Path(reports_dir, "reports.zip"), "w") as archive:
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

    def test_unreadable_child_does_not_block_healthy_sibling(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]
        api_client.instances.get_parent_study_id.return_value = "study-id"

        with tempfile.TemporaryDirectory() as temp_dir:
            unreadable_dir = Path(temp_dir, "a-unreadable")
            unreadable_dir.mkdir()
            image_path = Path(temp_dir, "image.dcm")
            image_path.write_bytes(b"dicom")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )
            real_listdir = os.listdir

            def listdir(path):
                if os.path.abspath(path) == os.path.abspath(unreadable_dir):
                    raise PermissionError("unreadable")
                return real_listdir(path)

            with mock.patch(
                "orthanc_tools.orthanc_folder_importer.os.listdir",
                side_effect=listdir,
            ):
                with self.assertRaisesRegex(RuntimeError, "folder entries"):
                    importer.upload_and_label(temp_dir)

        api_client.upload.assert_called_once_with(b"dicom", ignore_errors=True)

    def test_execute_propagates_worker_failures(self):
        api_client = mock.Mock()

        with tempfile.TemporaryDirectory() as temp_dir:
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            with mock.patch.object(
                importer,
                "upload_and_label",
                side_effect=PermissionError("unreadable"),
            ):
                with self.assertRaisesRegex(RuntimeError, "worker failed"):
                    importer.execute()

    def test_pdf_off_folder_traversal_skips_priority_scan(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "image.dcm").write_bytes(b"dicom")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
            )

            with mock.patch.object(
                importer,
                "_list_and_sort_dir",
                side_effect=AssertionError("priority scan should not run"),
            ):
                importer.upload_and_label(temp_dir)

        api_client.upload.assert_called_once_with(b"dicom", ignore_errors=True)

    def test_non_dcm_extension_report_pairs_keep_their_study(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "study-a.ima").write_bytes(b"dicom-a")
            Path(temp_dir, "study-a.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "study-b").write_bytes(b"dicom-b")
            Path(temp_dir, "study-b.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            [
                mock.call(
                    study_id="study-a",
                    pdf_path=str(Path(temp_dir, "study-a.pdf")),
                    series_description="PDF report",
                ),
                mock.call(
                    study_id="study-b",
                    pdf_path=str(Path(temp_dir, "study-b.pdf")),
                    series_description="PDF report",
                ),
            ],
            api_client.studies.attach_pdf.call_args_list,
        )

    def test_partial_child_failure_clears_previous_study_context(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "a-old.dcm").write_bytes(b"dicom-a")
            child_dir = Path(temp_dir, "b-child")
            unreadable_dir = Path(child_dir, "a-unreadable")
            unreadable_dir.mkdir(parents=True)
            Path(child_dir, "image.dcm").write_bytes(b"dicom-b")
            Path(temp_dir, "c-report.pdf").write_bytes(b"pdf")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )
            real_listdir = os.listdir

            def listdir(path):
                if os.path.abspath(path) == os.path.abspath(unreadable_dir):
                    raise PermissionError("unreadable")
                return real_listdir(path)

            with mock.patch(
                "orthanc_tools.orthanc_folder_importer.os.listdir",
                side_effect=listdir,
            ):
                with self.assertRaisesRegex(RuntimeError, "folder entries"):
                    importer.upload_and_label(temp_dir)

        api_client.studies.attach_pdf.assert_not_called()

    def test_sibling_image_report_directories_keep_their_study(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            for directory in ("a-images", "a-reports", "b-images", "b-reports"):
                Path(temp_dir, directory).mkdir()
            Path(temp_dir, "a-images", "image.dcm").write_bytes(b"dicom-a")
            Path(temp_dir, "a-reports", "report.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "b-images", "image.dcm").write_bytes(b"dicom-b")
            Path(temp_dir, "b-reports", "report.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            [
                mock.call(
                    study_id="study-a",
                    pdf_path=mock.ANY,
                    series_description="PDF report",
                ),
                mock.call(
                    study_id="study-b",
                    pdf_path=mock.ANY,
                    series_description="PDF report",
                ),
            ],
            api_client.studies.attach_pdf.call_args_list,
        )

    def test_filtered_dicom_clears_previous_study_context(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-a"]
        api_client.instances.get_parent_study_id.return_value = "study-a"

        class FilteringImporter(OrthancFolderImporter):
            def process_dicom_file(self, file_content):
                return None if file_content == b"dicom-b" else file_content

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "a.dcm").write_bytes(b"dicom-a")
            Path(temp_dir, "a.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "b.dcm").write_bytes(b"dicom-b")
            Path(temp_dir, "b.pdf").write_bytes(b"pdf-b")
            importer = FilteringImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(1, api_client.studies.attach_pdf.call_count)
        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-a",
            pdf_path=str(Path(temp_dir, "a.pdf")),
            series_description="PDF report",
        )

    def test_dotted_sibling_directory_ids_keep_their_study(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            for directory in (
                "study.1-images",
                "study.1-reports",
                "study.2-images",
                "study.2-reports",
            ):
                Path(temp_dir, directory).mkdir()
            Path(temp_dir, "study.1-images", "image.dcm").write_bytes(b"dicom-a")
            Path(temp_dir, "study.1-reports", "report.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "study.2-images", "image.dcm").write_bytes(b"dicom-b")
            Path(temp_dir, "study.2-reports", "report.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_centralized_role_directories_pair_descendant_studies(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            images_dir = Path(temp_dir, "images")
            reports_dir = Path(temp_dir, "reports")
            images_dir.mkdir()
            reports_dir.mkdir()
            Path(images_dir, "a.dcm").write_bytes(b"dicom-a")
            Path(images_dir, "b.dcm").write_bytes(b"dicom-b")
            Path(reports_dir, "a.pdf").write_bytes(b"pdf-a")
            Path(reports_dir, "b.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_multi_instance_stems_group_with_their_report(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [
            ["instance-a-1"],
            ["instance-a-2"],
            ["instance-b-1"],
            ["instance-b-2"],
        ]
        api_client.instances.get_parent_study_id.side_effect = [
            "study-a",
            "study-a",
            "study-b",
            "study-b",
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            for name in ("a-1.dcm", "a-2.dcm", "b-1.dcm", "b-2.dcm"):
                Path(temp_dir, name).write_bytes(b"dicom")
            Path(temp_dir, "a.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "b.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_file_role_suffixes_remain_distinct_pairing_ids(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-dicom"], ["instance-image"]]
        api_client.instances.get_parent_study_id.side_effect = [
            "study-dicom",
            "study-image",
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "case-dicom.dcm").write_bytes(b"dicom-a")
            Path(temp_dir, "case-dicom.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "case-image.dcm").write_bytes(b"dicom-b")
            Path(temp_dir, "case-image.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-dicom", "study-image"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_split_image_report_archives_keep_their_study(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            for study in ("a", "b"):
                with zipfile.ZipFile(Path(temp_dir, f"{study}-images.zip"), "w") as archive:
                    archive.writestr("image.dcm", b"dicom")
                with zipfile.ZipFile(Path(temp_dir, f"{study}-reports.zip"), "w") as archive:
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

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_split_archives_pair_multiple_descendant_studies(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(Path(temp_dir, "batch-images.zip"), "w") as archive:
                archive.writestr("a.dcm", b"dicom-a")
                archive.writestr("b.dcm", b"dicom-b")
            with zipfile.ZipFile(Path(temp_dir, "batch-reports.zip"), "w") as archive:
                archive.writestr("a.pdf", b"pdf-a")
                archive.writestr("b.pdf", b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_pdf_off_mixed_root_is_checkpointed_after_children(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-id"]

        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = Path(temp_dir, "input")
            child_dir = Path(input_dir, "child")
            child_dir.mkdir(parents=True)
            Path(input_dir, "root.dcm").write_bytes(b"dicom")
            Path(child_dir, "child.dcm").write_bytes(b"dicom")
            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=input_dir,
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=2,
            )

            importer.execute()

            self.assertCountEqual(
                [str(input_dir), str(child_dir)],
                state_path.read_text(encoding="utf-8").splitlines(),
            )

    def test_split_archives_ignore_skipped_metadata_when_pairing(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(Path(temp_dir, "batch-images.zip"), "w") as archive:
                archive.writestr("a.dcm", b"dicom-a")
                archive.writestr("b.dcm", b"dicom-b")
            with zipfile.ZipFile(Path(temp_dir, "batch-reports.zip"), "w") as archive:
                archive.writestr("a.pdf", b"pdf-a")
                archive.writestr("b.pdf", b"pdf-b")
                archive.writestr("README.txt", b"metadata")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                skip_extensions=[".txt"],
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_report_role_suffix_pairs_flat_studies(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "a.dcm").write_bytes(b"dicom-a")
            Path(temp_dir, "a-report.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "b.dcm").write_bytes(b"dicom-b")
            Path(temp_dir, "b-report.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_skipped_dicom_pair_does_not_reuse_previous_study(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-a"]
        api_client.instances.get_parent_study_id.return_value = "study-a"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "a.dcm").write_bytes(b"dicom-a")
            Path(temp_dir, "a.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "b.ima").write_bytes(b"dicom-b")
            Path(temp_dir, "b.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                skip_extensions=[".ima"],
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_nested_centralized_role_trees_pair_studies(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            for study in ("a", "b"):
                image_dir = Path(temp_dir, "images", "year", study)
                report_dir = Path(temp_dir, "reports", "year", study)
                image_dir.mkdir(parents=True)
                report_dir.mkdir(parents=True)
                Path(image_dir, "1.dcm").write_bytes(f"dicom-{study}".encode())
                Path(report_dir, "report.pdf").write_bytes(f"pdf-{study}".encode())
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_outer_archive_expands_nested_paired_archives(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            build_dir = Path(temp_dir, "build")
            build_dir.mkdir()
            images_path = Path(build_dir, "batch-images.zip")
            reports_path = Path(build_dir, "batch-reports.zip")
            with zipfile.ZipFile(images_path, "w") as archive:
                archive.writestr("a.dcm", b"dicom-a")
                archive.writestr("b.dcm", b"dicom-b")
            with zipfile.ZipFile(reports_path, "w") as archive:
                archive.writestr("a.pdf", b"pdf-a")
                archive.writestr("b.pdf", b"pdf-b")
            outer_path = Path(temp_dir, "outer.zip")
            with zipfile.ZipFile(outer_path, "w") as archive:
                archive.write(images_path, images_path.name)
                archive.write(reports_path, reports_path.name)
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(outer_path)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_shared_centralized_subfolder_preserves_file_pairing(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            image_dir = Path(temp_dir, "images", "year")
            report_dir = Path(temp_dir, "reports", "year")
            image_dir.mkdir(parents=True)
            report_dir.mkdir(parents=True)
            for study in ("a", "b"):
                Path(image_dir, f"{study}.dcm").write_bytes(f"dicom-{study}".encode())
                Path(report_dir, f"{study}.pdf").write_bytes(f"pdf-{study}".encode())
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_plural_pdfs_role_directory_pairs_studies(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            image_dir = Path(temp_dir, "images")
            report_dir = Path(temp_dir, "pdfs")
            image_dir.mkdir()
            report_dir.mkdir()
            for study in ("a", "b"):
                Path(image_dir, f"{study}.dcm").write_bytes(f"dicom-{study}".encode())
                Path(report_dir, f"{study}.pdf").write_bytes(f"pdf-{study}".encode())
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_multiple_report_aliases_stay_with_their_study(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "a.dcm").write_bytes(b"dicom-a")
            Path(temp_dir, "a.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "a-report.pdf").write_bytes(b"report-a")
            Path(temp_dir, "b.dcm").write_bytes(b"dicom-b")
            Path(temp_dir, "b.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_nested_archive_groups_include_their_parent_path(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-1"], ["instance-2"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-1", "study-2"]

        with tempfile.TemporaryDirectory() as temp_dir:
            for year in ("year1", "year2"):
                image_dir = Path(temp_dir, "images", year)
                report_dir = Path(temp_dir, "reports", year)
                image_dir.mkdir(parents=True)
                report_dir.mkdir(parents=True)
                with zipfile.ZipFile(Path(image_dir, "a.zip"), "w") as archive:
                    archive.writestr("image.dcm", f"dicom-{year}".encode())
                with zipfile.ZipFile(Path(report_dir, "a.zip"), "w") as archive:
                    archive.writestr("report.pdf", f"pdf-{year}".encode())
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-1", "study-2"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_suffix_qualified_role_directories_pair_studies(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            image_dir = Path(temp_dir, "batch-images")
            report_dir = Path(temp_dir, "batch-reports")
            image_dir.mkdir()
            report_dir.mkdir()
            for study in ("a", "b"):
                Path(image_dir, f"{study}.dcm").write_bytes(f"dicom-{study}".encode())
                Path(report_dir, f"{study}.pdf").write_bytes(f"pdf-{study}".encode())
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_skipped_multi_instance_member_clears_previous_study(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-a"]
        api_client.instances.get_parent_study_id.return_value = "study-a"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "a.dcm").write_bytes(b"dicom-a")
            Path(temp_dir, "a.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "b-1.ima").write_bytes(b"dicom-b")
            Path(temp_dir, "b.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                skip_extensions=[".ima"],
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_failed_later_instance_preserves_its_study_for_report(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], []]
        api_client.instances.get_parent_study_id.return_value = "study-a"
        api_client.is_alive.return_value = True

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "a-1.dcm").write_bytes(b"dicom-a-1")
            Path(temp_dir, "a-2.dcm").write_bytes(b"invalid-a-2")
            Path(temp_dir, "a.pdf").write_bytes(b"pdf-a")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_archives_under_suffix_role_directories_pair_studies(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            image_dir = Path(temp_dir, "batch-images")
            report_dir = Path(temp_dir, "batch-reports")
            image_dir.mkdir()
            report_dir.mkdir()
            with zipfile.ZipFile(Path(image_dir, "a.zip"), "w") as archive:
                archive.writestr("a.dcm", b"dicom-a")
                archive.writestr("b.dcm", b"dicom-b")
            with zipfile.ZipFile(Path(report_dir, "a.zip"), "w") as archive:
                archive.writestr("a.pdf", b"pdf-a")
                archive.writestr("b.pdf", b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-b"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )

    def test_grouped_archives_do_not_overwrite_same_named_members(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a-1"], ["instance-a-2"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-a"]

        with tempfile.TemporaryDirectory() as temp_dir:
            for archive_name, content in (
                ("a-images.zip", b"dicom-a-1"),
                ("a-dicom.zip", b"dicom-a-2"),
            ):
                with zipfile.ZipFile(Path(temp_dir, archive_name), "w") as archive:
                    archive.writestr("image.dcm", content)
            with zipfile.ZipFile(Path(temp_dir, "a-reports.zip"), "w") as archive:
                archive.writestr("report.pdf", b"pdf-a")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(2, api_client.upload.call_count)
        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-a",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_grouped_archives_align_by_member_study_names(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], ["instance-b"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-a", "study-b"]

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_members = (
                ("batch-dicom.zip", "a.dcm", b"dicom-a"),
                ("batch-images.zip", "b.dcm", b"dicom-b"),
                ("batch-pdf.zip", "b.pdf", b"pdf-b"),
                ("batch-reports.zip", "a.pdf", b"pdf-a"),
            )
            for archive_name, member_name, content in archive_members:
                with zipfile.ZipFile(Path(temp_dir, archive_name), "w") as archive:
                    archive.writestr(member_name, content)
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            {"a.pdf": "study-a", "b.pdf": "study-b"},
            {
                Path(call.kwargs["pdf_path"]).name: call.kwargs["study_id"]
                for call in api_client.studies.attach_pdf.call_args_list
            },
        )

    def test_paired_archive_sets_active_group_for_later_failed_instance(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-a"], []]
        api_client.instances.get_parent_study_id.return_value = "study-a"
        api_client.is_alive.return_value = True

        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(Path(temp_dir, "a-images.zip"), "w") as archive:
                archive.writestr("image.dcm", b"dicom-a")
            with zipfile.ZipFile(Path(temp_dir, "a-reports.zip"), "w") as archive:
                archive.writestr("report.pdf", b"pdf-a-1")
            Path(temp_dir, "a-z.dcm").write_bytes(b"invalid-a")
            Path(temp_dir, "a.pdf").write_bytes(b"pdf-a-2")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        self.assertEqual(
            ["study-a", "study-a"],
            [call.kwargs["study_id"] for call in api_client.studies.attach_pdf.call_args_list],
        )


if __name__ == "__main__":
    unittest.main()
