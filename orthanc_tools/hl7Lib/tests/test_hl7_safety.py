import errno
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import hl7
import pydicom

from orthanc_tools.hl7Lib.hl7_client import MLLPClient
from orthanc_tools.hl7Lib.hl7_dicom_worklist_builder import DicomWorklistBuilder
from orthanc_tools.hl7Lib.hl7_orm_worklist_msg_handler import Hl7OrmWorklistMsgHandler
from orthanc_tools.hl7Lib.hl7_oru_report_msg_handler import Hl7OruReportMsgHandler
from orthanc_tools.hl7Lib.hl7_server import handle_error_message


ORM_MESSAGE = (
    r"MSH|^~\&|SENDER|FACILITY|RECEIVER|DESTINATION|20260730120000||"
    "ORM^O01|orm-message-id|P|2.3\rPID|1"
)
ORU_MESSAGE = (
    r"MSH|^~\&|SENDER|FACILITY|RECEIVER|DESTINATION|20260730120000||"
    "ORU^R01|oru-message-id|P|2.3\rPID|1"
)
UNKNOWN_MESSAGE = (
    r"MSH|^~\&|SENDER|FACILITY|RECEIVER|DESTINATION|20260730120000||"
    "-------|unknown-message-id|P|2.3\rPID|1"
)


class FakeParser:
    def __init__(self, result=None, error=None):
        self._result = result or {}
        self._error = error

    def parse(self, hl7_message):
        if self._error:
            raise self._error
        return self._result


class FakeBuilder:
    _folder = "unused"
    _orthanc_client = None

    def __init__(self, error=None):
        self._error = error

    def generate(self, values):
        if self._error:
            raise self._error
        return "generated"


class TestHl7Acknowledgements(unittest.TestCase):
    def test_orm_success_acknowledgement_has_formatted_header(self):
        handler = Hl7OrmWorklistMsgHandler(
            parser=FakeParser(),
            builder=FakeBuilder(),
        )

        response = handler.handle_orm_message(ORM_MESSAGE)

        self.assertEqual("AA", response["MSA.F1.R1"])
        self.assertEqual("orm-message-id", response["MSA.F2.R1"])
        self.assertEqual("RECEIVER", response["MSH.F3.R1"])
        self.assertEqual("DESTINATION", response["MSH.F4.R1"])
        self.assertEqual("SENDER", response["MSH.F5.R1"])
        self.assertEqual("FACILITY", response["MSH.F6.R1"])
        self.assertEqual("ACK", response["MSH.F9.R1.C1"])
        self.assertEqual("O01", response["MSH.F9.R1.C2"])
        self.assertNotIn("{sending_application}", str(response))

    def test_orm_parser_failure_returns_application_error(self):
        handler = Hl7OrmWorklistMsgHandler(
            parser=FakeParser(error=ValueError("invalid order")),
            builder=FakeBuilder(),
        )

        response = handler.handle_orm_message(ORM_MESSAGE)

        self.assertEqual("AE", response["MSA.F1.R1"])
        self.assertEqual("invalid order", response["MSA.F3.R1"])

    def test_orm_builder_failure_returns_application_error(self):
        handler = Hl7OrmWorklistMsgHandler(
            parser=FakeParser(),
            builder=FakeBuilder(error=RuntimeError("disk full")),
        )

        response = handler.handle_orm_message(ORM_MESSAGE)

        self.assertEqual("AE", response["MSA.F1.R1"])
        self.assertEqual("disk full", response["MSA.F3.R1"])

    def test_oru_parser_failure_returns_application_error_for_r01(self):
        handler = Hl7OruReportMsgHandler(
            parser=FakeParser(error=ValueError("invalid report")),
            builder=FakeBuilder(),
        )

        response = handler.handle_oru_message(ORU_MESSAGE)

        self.assertEqual("AE", response["MSA.F1.R1"])
        self.assertEqual("ACK", response["MSH.F9.R1.C1"])
        self.assertEqual("R01", response["MSH.F9.R1.C2"])

    def test_default_error_handler_is_directly_callable(self):
        response = handle_error_message(
            ORM_MESSAGE,
            error_description="unsupported | message",
        )

        self.assertEqual("AR", response["MSA.F1.R1"])
        self.assertEqual("unsupported | message", response["MSA.F3.R1"])

    def test_error_acknowledgement_defaults_missing_trigger_event(self):
        response = handle_error_message(
            UNKNOWN_MESSAGE,
            error_description="unsupported message",
        )

        self.assertEqual("AR", response["MSA.F1.R1"])
        self.assertEqual("ACK", response["MSH.F9.R1.C1"])
        self.assertEqual("O01", response["MSH.F9.R1.C2"])

    def test_error_acknowledgement_allows_missing_control_id(self):
        response = handle_error_message(
            r"MSH|^~\&|S|F|R|D|||BAD",
            error_description="unsupported message",
        )

        self.assertEqual("AR", response["MSA.F1.R1"])
        self.assertEqual("", response["MSA.F2.R1"])

    def test_error_description_remains_encodable_for_mllp(self):
        response = handle_error_message(
            ORM_MESSAGE,
            error_description="invalid path 📁",
        )

        encoded_response = str(response).encode("iso-8859-1")

        self.assertIn(b"invalid path ?", encoded_response)


class TestMllpClientWrites(unittest.TestCase):
    def test_send_uses_sendall(self):
        client = object.__new__(MLLPClient)
        client.encoding = "iso-8859-1"
        client.sb = b"\x0b"
        client.eb = b"\x1c"
        client.cr = b"\r"
        client.socket = mock.Mock()
        client._receive = mock.Mock(return_value="response")
        message = hl7.parse(ORM_MESSAGE)

        self.assertEqual("response", client.send(message))

        client.socket.sendall.assert_called_once()
        client.socket.send.assert_not_called()


class TestWorklistFileSafety(unittest.TestCase):
    @staticmethod
    def _values(accession_number):
        return {
            "AccessionNumber": accession_number,
            "PatientID": "patient",
            "PatientName": "Patient^Test",
            "PatientBirthDate": "20000101",
            "PatientSex": "O",
            "RequestedProcedureID": "procedure",
            "SpecificCharacterSet": "ISO_IR 100",
            "ScheduledStationAETitle": "ORTHANC",
            "ScheduledProcedureStepID": "step",
        }

    def test_accession_number_cannot_escape_worklist_folder(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            builder = DicomWorklistBuilder(folder=temporary_dir)

            file_name = builder.generate(self._values("../outside"))

            self.assertEqual(
                os.path.realpath(temporary_dir),
                os.path.dirname(os.path.realpath(file_name)),
            )
            self.assertTrue(os.path.isfile(file_name))
            self.assertFalse(os.path.exists(os.path.join(temporary_dir, "..", "outside.wl")))

    def test_failed_write_does_not_leave_partial_file(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            builder = DicomWorklistBuilder(folder=temporary_dir)

            with mock.patch.object(
                pydicom.dataset.FileDataset,
                "save_as",
                side_effect=RuntimeError("write failed"),
            ):
                with self.assertRaisesRegex(RuntimeError, "write failed"):
                    builder.generate(self._values("safe-accession"))

            self.assertEqual([], os.listdir(temporary_dir))

    def test_long_explicit_filename_uses_bounded_temporary_name(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            output_path = os.path.join(temporary_dir, f"{'a' * 220}.wl")

            returned_path = DicomWorklistBuilder().generate(
                self._values("safe-accession"),
                file_name=output_path,
            )

            self.assertEqual(output_path, returned_path)
            self.assertEqual("patient", pydicom.dcmread(output_path).PatientID)

    def test_sanitized_accession_numbers_do_not_collide(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            builder = DicomWorklistBuilder(folder=temporary_dir)

            spaced_path = builder.generate(self._values("ABC 123"))
            underscored_path = builder.generate(self._values("ABC_123"))

            self.assertNotEqual(spaced_path, underscored_path)
            self.assertTrue(os.path.isfile(spaced_path))
            self.assertTrue(os.path.isfile(underscored_path))

    def test_existing_legacy_filename_is_reused(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            legacy_path = os.path.join(temporary_dir, "ABC 123.wl")
            DicomWorklistBuilder().generate(
                self._values("ABC 123"),
                file_name=legacy_path,
            )

            returned_path = DicomWorklistBuilder(folder=temporary_dir).generate(
                self._values("ABC 123")
            )

            self.assertEqual(legacy_path, returned_path)
            self.assertFalse(os.path.exists(os.path.join(temporary_dir, "ABC%20123.wl")))

    def test_encoded_filename_cannot_replace_different_legacy_accession(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            colliding_path = os.path.join(temporary_dir, "ABC%20123.wl")
            DicomWorklistBuilder().generate(
                self._values("ABC%20123"),
                file_name=colliding_path,
            )

            with self.assertRaisesRegex(ValueError, "different accession"):
                DicomWorklistBuilder(folder=temporary_dir).generate(
                    self._values("ABC 123")
                )

            self.assertEqual(
                "ABC%20123",
                pydicom.dcmread(colliding_path).AccessionNumber,
            )

    def test_encoded_filename_is_not_reused_as_another_legacy_name(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            builder = DicomWorklistBuilder(folder=temporary_dir)
            spaced_path = builder.generate(self._values("ABC 123"))

            percent_path = builder.generate(self._values("ABC%20123"))

            self.assertNotEqual(spaced_path, percent_path)
            self.assertEqual("ABC 123", pydicom.dcmread(spaced_path).AccessionNumber)
            self.assertEqual("ABC%20123", pydicom.dcmread(percent_path).AccessionNumber)

    def test_missing_accession_number_uses_generated_uid_filename(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            builder = DicomWorklistBuilder(folder=temporary_dir)

            output_path = builder.generate(self._values(None))

            self.assertTrue(os.path.isfile(output_path))
            self.assertNotEqual(".wl", os.path.basename(output_path))

    @unittest.skipIf(os.name == "nt", "Windows does not preserve POSIX permission bits")
    def test_new_worklist_respects_process_umask(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            previous_umask = os.umask(0o077)
            try:
                builder = DicomWorklistBuilder(folder=temporary_dir)
                output_path = builder.generate(self._values("safe-accession"))
            finally:
                os.umask(previous_umask)

            self.assertEqual(0o600, stat.S_IMODE(os.stat(output_path).st_mode))

    @unittest.skipIf(os.name == "nt", "Windows does not preserve POSIX permission bits")
    def test_replacing_worklist_preserves_existing_permissions(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            output_path = os.path.join(temporary_dir, "safe-accession.wl")
            with open(output_path, "wb") as output_file:
                output_file.write(b"existing")
            os.chmod(output_path, 0o600)

            builder = DicomWorklistBuilder(folder=temporary_dir)
            builder.generate(self._values("safe-accession"))

            self.assertEqual(0o600, stat.S_IMODE(os.stat(output_path).st_mode))

    @unittest.skipUnless(hasattr(os, "fchown"), "ownership changes are unavailable")
    def test_replacement_falls_back_when_owner_cannot_be_assumed(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            output_path = os.path.join(temporary_dir, "safe-accession.wl")
            with open(output_path, "wb") as output_file:
                output_file.write(b"existing")

            builder = DicomWorklistBuilder(folder=temporary_dir)
            with mock.patch(
                "orthanc_tools.hl7Lib.hl7_dicom_worklist_builder.os.fchown",
                side_effect=PermissionError("foreign owner"),
            ):
                builder.generate(self._values("safe-accession"))

            self.assertEqual("patient", pydicom.dcmread(output_path).PatientID)
            self.assertEqual(
                ["safe-accession.wl"],
                os.listdir(temporary_dir),
            )

    @unittest.skipUnless(hasattr(os, "fchown"), "ownership changes are unavailable")
    def test_metadata_fallback_rejects_destination_swap(self):
        with tempfile.TemporaryDirectory() as parent_dir:
            worklist_dir = os.path.join(parent_dir, "worklists")
            os.mkdir(worklist_dir)
            output_path = os.path.join(worklist_dir, "safe-accession.wl")
            outside_path = os.path.join(parent_dir, "outside.wl")
            Path(output_path).write_bytes(b"existing")
            Path(outside_path).write_bytes(b"outside")
            original_open = os.open

            def swap_destination_before_open(path, flags, *args, **kwargs):
                if Path(path) == Path(output_path):
                    os.unlink(output_path)
                    os.symlink(outside_path, output_path)
                return original_open(path, flags, *args, **kwargs)

            builder = DicomWorklistBuilder(folder=worklist_dir)
            with mock.patch(
                "orthanc_tools.hl7Lib.hl7_dicom_worklist_builder.os.fchown",
                side_effect=PermissionError("foreign owner"),
            ), mock.patch(
                "orthanc_tools.hl7Lib.hl7_dicom_worklist_builder.os.open",
                side_effect=swap_destination_before_open,
            ):
                with self.assertRaisesRegex(ValueError, "destination changed"):
                    builder.generate(self._values("safe-accession"))

            self.assertEqual(b"outside", Path(outside_path).read_bytes())

    @unittest.skipUnless(hasattr(os, "fchown"), "descriptor ownership is unavailable")
    def test_temporary_metadata_rejects_replaced_entry(self):
        with tempfile.TemporaryDirectory() as parent_dir:
            worklist_dir = Path(parent_dir) / "worklists"
            worklist_dir.mkdir()
            output_path = worklist_dir / "safe-accession.wl"
            outside_path = Path(parent_dir) / "outside.wl"
            output_path.write_bytes(b"existing")
            outside_path.write_bytes(b"outside")
            original_fchown = os.fchown

            def replace_temporary_entry(fd, uid, gid):
                original_fchown(fd, uid, gid)
                temp_path = next(worklist_dir.glob(".*.tmp"))
                temp_path.unlink()
                temp_path.symlink_to(outside_path)

            builder = DicomWorklistBuilder(folder=os.fspath(worklist_dir))
            with mock.patch(
                "orthanc_tools.hl7Lib.hl7_dicom_worklist_builder.os.fchown",
                side_effect=replace_temporary_entry,
            ):
                with self.assertRaisesRegex(ValueError, "changed before promotion"):
                    builder.generate(self._values("safe-accession"))

            self.assertEqual(b"outside", outside_path.read_bytes())
            self.assertEqual(b"existing", output_path.read_bytes())

    def test_explicit_symlink_updates_its_target(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            target_path = os.path.join(temporary_dir, "target.wl")
            link_path = os.path.join(temporary_dir, "configured.wl")
            with open(target_path, "wb") as target_file:
                target_file.write(b"existing")
            try:
                os.symlink(target_path, link_path)
            except (OSError, NotImplementedError) as ex:
                self.skipTest(f"symbolic links are unavailable: {ex}")

            builder = DicomWorklistBuilder()
            returned_path = builder.generate(
                self._values("safe-accession"),
                file_name=link_path,
            )

            self.assertEqual(link_path, returned_path)
            self.assertTrue(os.path.islink(link_path))
            self.assertEqual("patient", pydicom.dcmread(target_path).PatientID)

    def test_automatic_symlink_updates_target_inside_worklist_folder(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            archive_dir = os.path.join(temporary_dir, "archive")
            os.mkdir(archive_dir)
            target_path = os.path.join(archive_dir, "safe-accession.wl")
            link_path = os.path.join(temporary_dir, "safe-accession.wl")
            with open(target_path, "wb") as target_file:
                target_file.write(b"existing")
            try:
                os.symlink(target_path, link_path)
            except (OSError, NotImplementedError) as ex:
                self.skipTest(f"symbolic links are unavailable: {ex}")

            builder = DicomWorklistBuilder(folder=temporary_dir)
            returned_path = builder.generate(self._values("safe-accession"))

            self.assertEqual(link_path, returned_path)
            self.assertTrue(os.path.islink(link_path))
            self.assertEqual("patient", pydicom.dcmread(target_path).PatientID)

    def test_automatic_symlink_cannot_escape_worklist_folder(self):
        with tempfile.TemporaryDirectory() as parent_dir:
            worklist_dir = os.path.join(parent_dir, "worklists")
            os.mkdir(worklist_dir)
            target_path = os.path.join(parent_dir, "outside.wl")
            link_path = os.path.join(worklist_dir, "safe-accession.wl")
            with open(target_path, "wb") as target_file:
                target_file.write(b"existing")
            try:
                os.symlink(target_path, link_path)
            except (OSError, NotImplementedError) as ex:
                self.skipTest(f"symbolic links are unavailable: {ex}")

            builder = DicomWorklistBuilder(folder=worklist_dir)
            with self.assertRaisesRegex(ValueError, "must remain inside"):
                builder.generate(self._values("safe-accession"))

            self.assertEqual(b"existing", Path(target_path).read_bytes())

    def test_automatic_symlink_swap_cannot_escape_worklist_folder(self):
        with tempfile.TemporaryDirectory() as parent_dir:
            worklist_dir = os.path.join(parent_dir, "worklists")
            os.mkdir(worklist_dir)
            inside_path = os.path.join(worklist_dir, "inside.wl")
            outside_path = os.path.join(parent_dir, "outside.wl")
            link_path = os.path.join(worklist_dir, "safe-accession.wl")
            Path(inside_path).write_bytes(b"inside")
            Path(outside_path).write_bytes(b"outside")
            try:
                os.symlink(inside_path, link_path)
            except (OSError, NotImplementedError) as ex:
                self.skipTest(f"symbolic links are unavailable: {ex}")

            original_resolve = Path.resolve
            link_resolutions = 0

            def swap_link_before_second_resolution(path, *args, **kwargs):
                nonlocal link_resolutions
                if path == Path(link_path):
                    link_resolutions += 1
                    if link_resolutions == 2:
                        os.unlink(link_path)
                        os.symlink(outside_path, link_path)
                return original_resolve(path, *args, **kwargs)

            builder = DicomWorklistBuilder(folder=worklist_dir)
            with mock.patch.object(Path, "resolve", swap_link_before_second_resolution):
                with self.assertRaisesRegex(ValueError, "must remain inside"):
                    builder.generate(self._values("safe-accession"))

            self.assertEqual(b"outside", Path(outside_path).read_bytes())

    def test_automatic_parent_symlink_swap_cannot_escape_worklist_folder(self):
        with tempfile.TemporaryDirectory() as parent_dir:
            worklist_dir = Path(parent_dir) / "worklists"
            inside_dir = worklist_dir / "inside"
            outside_dir = Path(parent_dir) / "outside"
            archive_link = worklist_dir / "archive"
            worklist_dir.mkdir()
            inside_dir.mkdir()
            outside_dir.mkdir()
            try:
                archive_link.symlink_to(inside_dir, target_is_directory=True)
            except (OSError, NotImplementedError) as ex:
                self.skipTest(f"symbolic links are unavailable: {ex}")

            legacy_path = archive_link / "ACC.wl"
            DicomWorklistBuilder().generate(
                self._values("archive/ACC"),
                file_name=os.fspath(inside_dir / "ACC.wl"),
            )
            original_resolve = Path.resolve
            legacy_resolutions = 0

            def swap_parent_before_second_resolution(path, *args, **kwargs):
                nonlocal legacy_resolutions
                if path == legacy_path:
                    legacy_resolutions += 1
                    if legacy_resolutions == 2:
                        archive_link.unlink()
                        archive_link.symlink_to(outside_dir, target_is_directory=True)
                return original_resolve(path, *args, **kwargs)

            builder = DicomWorklistBuilder(folder=os.fspath(worklist_dir))
            with mock.patch.object(Path, "resolve", swap_parent_before_second_resolution):
                with self.assertRaisesRegex(ValueError, "must remain inside"):
                    builder.generate(self._values("archive/ACC"))

            self.assertFalse((outside_dir / "ACC.wl").exists())

    def test_nested_automatic_destination_rejects_parent_swap_before_open(self):
        with tempfile.TemporaryDirectory() as parent_dir:
            worklist_dir = Path(parent_dir) / "worklists"
            nested_dir = worklist_dir / "nested"
            moved_dir = worklist_dir / "moved"
            outside_dir = Path(parent_dir) / "outside"
            nested_dir.mkdir(parents=True)
            outside_dir.mkdir()
            output_path = nested_dir / "ACC.wl"
            outside_path = outside_dir / "ACC.wl"
            DicomWorklistBuilder().generate(
                self._values("nested/ACC"),
                file_name=os.fspath(output_path),
            )
            outside_path.write_bytes(b"outside")
            original_open = os.open

            def swap_parent_before_open(path, flags, *args, **kwargs):
                if Path(path) == output_path:
                    nested_dir.rename(moved_dir)
                    nested_dir.symlink_to(outside_dir, target_is_directory=True)
                return original_open(path, flags, *args, **kwargs)

            builder = DicomWorklistBuilder(folder=os.fspath(worklist_dir))
            with mock.patch(
                "orthanc_tools.hl7Lib.hl7_dicom_worklist_builder.os.open",
                side_effect=swap_parent_before_open,
            ):
                with self.assertRaisesRegex(ValueError, "destination changed"):
                    builder.generate(self._values("nested/ACC"))

            self.assertEqual(b"outside", outside_path.read_bytes())

    def test_hard_linked_destination_updates_all_links(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            output_path = os.path.join(temporary_dir, "safe-accession.wl")
            linked_path = os.path.join(temporary_dir, "linked.wl")
            with open(output_path, "wb") as output_file:
                output_file.write(b"existing")
            try:
                os.link(output_path, linked_path)
            except (OSError, NotImplementedError) as ex:
                self.skipTest(f"hard links are unavailable: {ex}")

            builder = DicomWorklistBuilder(folder=temporary_dir)
            builder.generate(self._values("safe-accession"))

            self.assertEqual("patient", pydicom.dcmread(linked_path).PatientID)
            self.assertEqual(os.stat(output_path).st_ino, os.stat(linked_path).st_ino)

    def test_windows_replacement_preserves_existing_inode(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            output_path = os.path.join(temporary_dir, "safe-accession.wl")
            with open(output_path, "wb") as output_file:
                output_file.write(b"existing")
            original_inode = os.stat(output_path).st_ino

            builder = DicomWorklistBuilder(folder=temporary_dir)
            with mock.patch(
                "orthanc_tools.hl7Lib.hl7_dicom_worklist_builder._IS_WINDOWS",
                True,
            ):
                builder.generate(self._values("safe-accession"))

            self.assertEqual(original_inode, os.stat(output_path).st_ino)
            self.assertEqual("patient", pydicom.dcmread(output_path).PatientID)

    def test_in_place_write_rejects_destination_swap(self):
        with tempfile.TemporaryDirectory() as parent_dir:
            worklist_dir = os.path.join(parent_dir, "worklists")
            os.mkdir(worklist_dir)
            output_path = os.path.join(worklist_dir, "safe-accession.wl")
            linked_path = os.path.join(worklist_dir, "linked.wl")
            outside_path = os.path.join(parent_dir, "outside.wl")
            Path(output_path).write_bytes(b"existing")
            Path(outside_path).write_bytes(b"outside")
            try:
                os.link(output_path, linked_path)
            except (OSError, NotImplementedError) as ex:
                self.skipTest(f"hard links are unavailable: {ex}")

            original_open = os.open

            def swap_destination_before_open(path, flags, *args, **kwargs):
                if Path(path) == Path(output_path):
                    os.unlink(output_path)
                    os.symlink(outside_path, output_path)
                return original_open(path, flags, *args, **kwargs)

            builder = DicomWorklistBuilder(folder=worklist_dir)
            with mock.patch(
                "orthanc_tools.hl7Lib.hl7_dicom_worklist_builder.os.open",
                side_effect=swap_destination_before_open,
            ):
                with self.assertRaisesRegex(ValueError, "destination changed"):
                    builder.generate(self._values("safe-accession"))

            self.assertEqual(b"outside", Path(outside_path).read_bytes())
            self.assertEqual(b"existing", Path(linked_path).read_bytes())

    @unittest.skipUnless(
        all(hasattr(os, name) for name in ("listxattr", "getxattr", "setxattr")),
        "extended attributes are unavailable",
    )
    def test_replacing_worklist_preserves_extended_attributes(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            output_path = os.path.join(temporary_dir, "safe-accession.wl")
            with open(output_path, "wb") as output_file:
                output_file.write(b"existing")
            try:
                os.setxattr(output_path, "user.orthanc-test", b"preserve")
            except OSError as ex:
                self.skipTest(f"extended attributes are unavailable: {ex}")

            builder = DicomWorklistBuilder(folder=temporary_dir)
            builder.generate(self._values("safe-accession"))

            self.assertEqual(
                b"preserve",
                os.getxattr(output_path, "user.orthanc-test"),
            )

    @unittest.skipUnless(
        all(hasattr(os, name) for name in ("listxattr", "getxattr", "setxattr")),
        "extended attributes are unavailable",
    )
    def test_unsupported_extended_attributes_do_not_block_replacement(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            output_path = os.path.join(temporary_dir, "safe-accession.wl")
            with open(output_path, "wb") as output_file:
                output_file.write(b"existing")
            builder = DicomWorklistBuilder(folder=temporary_dir)

            with mock.patch(
                "orthanc_tools.hl7Lib.hl7_dicom_worklist_builder.os.listxattr",
                side_effect=OSError(errno.ENOTSUP, "xattrs unsupported"),
            ):
                builder.generate(self._values("safe-accession"))

            self.assertEqual("patient", pydicom.dcmread(output_path).PatientID)


if __name__ == "__main__":
    unittest.main()
