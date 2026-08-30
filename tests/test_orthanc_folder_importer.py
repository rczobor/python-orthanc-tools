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

    def test_regular_import_does_not_lookup_parent_without_labels(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-1"]

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "image.dcm")
            input_path.write_bytes(b"dicom")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
            )

            importer.upload_and_label(str(input_path))

        api_client.instances.get_parent_study_id.assert_not_called()

    def test_zip_pdf_uses_the_study_uploaded_from_the_same_archive(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
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

    def test_zip_pdf_validates_all_studies_before_attaching_nested_pdf(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-1"], ["instance-2"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-1", "study-2"]

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_path = Path(temp_dir, "study.zip")
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("nested/image.dcm", b"dicom-1")
                archive.writestr("nested/report.pdf", b"pdf")
                archive.writestr("image.dcm", b"dicom-2")

            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            with self.assertRaisesRegex(RuntimeError, "multiple studies"):
                importer.upload_and_label(str(archive_path))

        api_client.studies.attach_pdf.assert_not_called()

    def test_pdf_import_ignores_a_nested_zip_when_configured_to_skip_it(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "image.dcm").write_bytes(b"dicom")
            Path(temp_dir, "report.pdf").write_bytes(b"pdf")
            with zipfile.ZipFile(Path(temp_dir, "backup.zip"), "w") as archive:
                archive.writestr("backup.dcm", b"backup")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                skip_extensions=[".zip"],
                dicomize_pdf=True,
            )

            importer.upload_and_label(temp_dir)

        api_client.upload.assert_called_once_with(b"dicom", ignore_errors=True)
        api_client.studies.attach_pdf.assert_called_once()

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

    def test_pdf_import_rejects_multiple_pdfs_before_uploading(self):
        api_client = mock.Mock()

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "image.dcm").write_bytes(b"dicom")
            Path(temp_dir, "report-a.pdf").write_bytes(b"pdf-a")
            Path(temp_dir, "report-b.pdf").write_bytes(b"pdf-b")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                max_retries=0,
                dicomize_pdf=True,
            )

            with self.assertRaisesRegex(RuntimeError, "one PDF"):
                importer.upload_and_label(temp_dir)

        api_client.upload.assert_not_called()
        api_client.studies.attach_pdf.assert_not_called()

    def test_pdf_import_propagates_subtree_traversal_errors(self):
        api_client = mock.Mock()

        def failed_walk(_path, onerror):
            onerror(PermissionError("denied"))
            return []

        with tempfile.TemporaryDirectory() as temp_dir:
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                dicomize_pdf=True,
            )

            with mock.patch("orthanc_tools.orthanc_folder_importer.os.walk", failed_walk):
                with self.assertRaisesRegex(PermissionError, "denied"):
                    importer.upload_and_label(temp_dir)

        api_client.upload.assert_not_called()

    def test_pdf_import_rejects_symlinked_subdirectories(self):
        api_client = mock.Mock()

        with tempfile.TemporaryDirectory() as temp_dir:
            linked_path = Path(temp_dir, "linked")
            linked_path.mkdir()
            Path(linked_path, "image.dcm").write_bytes(b"dicom")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                dicomize_pdf=True,
            )

            with mock.patch(
                "orthanc_tools.orthanc_folder_importer.os.path.islink",
                side_effect=lambda path: Path(path) == linked_path,
            ):
                with self.assertRaisesRegex(RuntimeError, "symbolic link"):
                    importer.upload_and_label(temp_dir)

        api_client.upload.assert_not_called()

    def test_pdf_import_fails_when_an_enumerated_file_disappears(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "image.dcm").write_bytes(b"dicom")
            report_path = Path(temp_dir, "report.pdf")
            report_path.write_bytes(b"pdf")
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
            list_pdf_import_files = importer._list_pdf_import_files

            def enumerate_then_remove(path):
                paths = list_pdf_import_files(path)
                report_path.unlink()
                return paths

            with mock.patch.object(
                importer,
                "_list_pdf_import_files",
                side_effect=enumerate_then_remove,
            ):
                with self.assertRaisesRegex(RuntimeError, "Importer worker failed"):
                    importer.execute()

            self.assertFalse(state_path.exists())

        api_client.upload.assert_called_once_with(b"dicom", ignore_errors=True)
        api_client.studies.attach_pdf.assert_not_called()

    def test_pdf_import_validates_root_study_before_attaching_nested_pdf(self):
        api_client = mock.Mock()
        api_client.upload.side_effect = [["instance-1"], ["instance-2"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-1", "study-2"]

        with tempfile.TemporaryDirectory() as temp_dir:
            nested_path = Path(temp_dir, "nested")
            nested_path.mkdir()
            Path(nested_path, "image.dcm").write_bytes(b"dicom-1")
            Path(nested_path, "report.pdf").write_bytes(b"pdf")
            Path(temp_dir, "image.dcm").write_bytes(b"dicom-2")
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

    def test_pdf_mode_reprocesses_an_unterminated_state_record(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "input")
            completed_path = Path(input_path, "completed")
            completed_path.mkdir(parents=True)
            unit_path = Path(input_path, "study")
            unit_path.mkdir()
            Path(unit_path, "image.dcm").write_bytes(b"dicom")
            state_path = Path(temp_dir, "state.txt")
            state_path.write_text(f"{completed_path}\n{unit_path}")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=str(input_path),
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

            self.assertEqual(
                [str(completed_path), str(unit_path)],
                state_path.read_text().splitlines(),
            )

        api_client.upload.assert_called_once_with(b"dicom", ignore_errors=True)

    def test_pdf_attachment_is_retried_before_checkpointing(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
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

    def test_existing_matching_pdf_is_not_attached_again(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"
        api_client.studies.get_pdf_instances.return_value = ["pdf-instance"]

        def download_pdf(_instance_id, destination_path):
            Path(destination_path).write_bytes(b"pdf")

        api_client.instances.download_pdf.side_effect = download_pdf

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "image.dcm").write_bytes(b"dicom")
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

            importer.execute()

            self.assertEqual([temp_dir], state_path.read_text().splitlines())

        api_client.studies.attach_pdf.assert_not_called()

    def test_pdf_attachment_uses_hashed_snapshot_and_rejects_source_replacement(self):
        api_client = mock.Mock()
        attached_content = []

        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir, "report.pdf")
            report_path.write_bytes(b"original")
            replacement_path = Path(temp_dir, "replacement.pdf")
            replacement_path.write_bytes(b"replacement")

            def replace_source(_study_id, max_instance_count_in_series_to_analyze):
                os.replace(replacement_path, report_path)
                return []

            def capture_attachment(study_id, pdf_path, series_description):
                attached_content.append(Path(pdf_path).read_bytes())

            api_client.studies.get_pdf_instances.side_effect = replace_source
            api_client.studies.attach_pdf.side_effect = capture_attachment
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=None,
                dicomize_pdf=True,
            )

            with self.assertRaisesRegex(RuntimeError, "changed while it was being imported"):
                importer._attach_pdf_idempotently("study-1", str(report_path))

        self.assertEqual([b"original"], attached_content)

    def test_failed_pdf_does_not_checkpoint_a_nested_dicom_folder(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
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

    def test_non_pdf_mode_does_not_checkpoint_mutable_import_root(self):
        api_client = mock.Mock()
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "image.dcm").write_bytes(b"dicom")
            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=temp_dir,
                errors_path=None,
                state_path=str(state_path),
                worker_threads_count=1,
            )

            importer.execute()

            self.assertFalse(state_path.exists())

    def test_pdf_mode_treats_sibling_folders_as_independent_units(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
        api_client.upload.side_effect = [["instance-1"], ["instance-2"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-1", "study-2"]

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "input")
            input_path.mkdir()
            for name in ("a", "b"):
                unit_path = Path(input_path, name)
                unit_path.mkdir()
                Path(unit_path, "image.dcm").write_bytes(b"dicom")
                Path(unit_path, "report.pdf").write_bytes(b"pdf")
            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=str(input_path),
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

            self.assertEqual(
                [str(Path(input_path, "a")), str(Path(input_path, "b"))],
                state_path.read_text().splitlines(),
            )

        self.assertEqual(
            [mock.call(study_id="study-1", pdf_path=mock.ANY, series_description="PDF report"),
             mock.call(study_id="study-2", pdf_path=mock.ANY, series_description="PDF report")],
            api_client.studies.attach_pdf.call_args_list,
        )

    def test_pdf_mode_ignores_state_files_when_resuming_sibling_units(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
        api_client.upload.return_value = ["instance-2"]
        api_client.instances.get_parent_study_id.return_value = "study-2"

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "input")
            input_path.mkdir()
            for name in ("a", "b"):
                unit_path = Path(input_path, name)
                unit_path.mkdir()
                Path(unit_path, "image.dcm").write_bytes(b"dicom")
                Path(unit_path, "report.pdf").write_bytes(b"pdf")

            state_path = Path(input_path, "state.txt")
            errors_path = Path(input_path, "errors.txt")
            state_path.write_text(str(Path(input_path, "a")) + "\n")
            errors_path.write_text("previous failure\n")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=str(input_path),
                errors_path=str(errors_path),
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

            self.assertEqual(
                [str(Path(input_path, "a")), str(Path(input_path, "b"))],
                state_path.read_text().splitlines(),
            )

        api_client.upload.assert_called_once_with(b"dicom", ignore_errors=True)
        api_client.studies.attach_pdf.assert_called_once()

    def test_pdf_mode_ignores_skipped_root_files_when_selecting_units(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
        api_client.upload.side_effect = [["instance-1"], ["instance-2"]]
        api_client.instances.get_parent_study_id.side_effect = ["study-1", "study-2"]

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "input")
            input_path.mkdir()
            readme_path = Path(input_path, "README.txt")
            readme_path.write_text("metadata")
            for name in ("a", "b"):
                unit_path = Path(input_path, name)
                unit_path.mkdir()
                Path(unit_path, "image.dcm").write_bytes(b"dicom")
                Path(unit_path, "report.pdf").write_bytes(b"pdf")

            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=str(input_path),
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                skip_extensions=[".txt"],
                dicomize_pdf=True,
            )

            with mock.patch(
                "orthanc_tools.orthanc_folder_importer.os.path.islink",
                side_effect=lambda path: Path(path) == readme_path,
            ):
                importer.execute()

            self.assertEqual(
                [str(Path(input_path, "a")), str(Path(input_path, "b"))],
                state_path.read_text().splitlines(),
            )

        self.assertEqual(2, api_client.upload.call_count)
        self.assertEqual(2, api_client.studies.attach_pdf.call_count)

    def test_pdf_mode_ignores_state_files_inside_a_root_unit(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "input")
            input_path.mkdir()
            Path(input_path, "image.dcm").write_bytes(b"dicom")
            Path(input_path, "report.pdf").write_bytes(b"pdf")
            state_path = Path(input_path, "state.txt")
            errors_path = Path(input_path, "errors.txt")
            errors_path.write_text("previous failure\n")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=str(input_path),
                errors_path=str(errors_path),
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

            self.assertEqual([str(input_path)], state_path.read_text().splitlines())

        api_client.upload.assert_called_once_with(b"dicom", ignore_errors=True)
        api_client.studies.attach_pdf.assert_called_once()

    def test_pdf_mode_imports_and_checkpoints_a_root_zip_unit(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "input")
            input_path.mkdir()
            archive_path = Path(input_path, "study.zip")
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("report.pdf", b"pdf")
                archive.writestr("image.dcm", b"dicom")
            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=str(input_path),
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

            self.assertEqual([str(archive_path)], state_path.read_text().splitlines())

        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-1",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_pdf_mode_groups_extensionless_root_dicom_with_nested_pdf(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "input")
            input_path.mkdir()
            Path(input_path, "image.ima").write_bytes(b"dicom")
            report_path = Path(input_path, "reports")
            report_path.mkdir()
            Path(report_path, "report.pdf").write_bytes(b"pdf")
            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=str(input_path),
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

            self.assertEqual([str(input_path)], state_path.read_text().splitlines())

        api_client.studies.attach_pdf.assert_called_once_with(
            study_id="study-1",
            pdf_path=mock.ANY,
            series_description="PDF report",
        )

    def test_pdf_mode_groups_non_archive_zip_named_dicom_with_nested_pdf(self):
        api_client = mock.Mock()
        api_client.studies.get_pdf_instances.return_value = []
        api_client.upload.return_value = ["instance-1"]
        api_client.instances.get_parent_study_id.return_value = "study-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir, "input")
            input_path.mkdir()
            Path(input_path, "image.zip").write_bytes(b"dicom")
            report_path = Path(input_path, "reports")
            report_path.mkdir()
            Path(report_path, "report.pdf").write_bytes(b"pdf")
            state_path = Path(temp_dir, "state.txt")
            importer = OrthancFolderImporter(
                api_client=api_client,
                folder_path=str(input_path),
                errors_path=None,
                state_path=str(state_path),
                max_retries=0,
                worker_threads_count=1,
                dicomize_pdf=True,
            )

            importer.execute()

            self.assertEqual([str(input_path)], state_path.read_text().splitlines())

        api_client.upload.assert_called_once_with(b"dicom", ignore_errors=True)
        api_client.studies.attach_pdf.assert_called_once()

    def test_cli_help_builds_parser(self):
        result = subprocess.run(
            [sys.executable, "-m", "orthanc_tools.orthanc_folder_importer", "--help"],
            capture_output=True,
            text=True,
        )

        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
