import datetime
import errno
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from orthanc_tools.helpers.scheduler import Scheduler
from orthanc_tools.orthanc_syncher import OrthancSyncher


class TestSchedulerWaitResult(unittest.TestCase):
    def test_reports_when_it_waited_for_a_running_period(self):
        scheduler = Scheduler()
        scheduler._running_periods.is_in_period = mock.Mock(
            side_effect=[False, True]
        )

        with mock.patch("orthanc_tools.helpers.scheduler.time.sleep"):
            waited = scheduler.wait_right_time_to_run()

        self.assertTrue(waited)


class TestOrthancSyncherSafety(unittest.TestCase):
    def _syncher(self, **kwargs):
        return OrthancSyncher(
            api_client_1=mock.MagicMock(),
            api_client_2=mock.MagicMock(),
            **kwargs,
        )

    def test_empty_source_preserves_last_update_limit(self):
        last_update_limit = datetime.datetime(2026, 7, 30, 12, 0, 0)
        syncher = self._syncher()
        syncher.get_studies = mock.Mock(return_value=[])

        result = syncher.synch(
            orthanc_source=mock.sentinel.source,
            orthanc_destination=mock.sentinel.destination,
            last_update_limit=last_update_limit,
        )

        self.assertEqual(last_update_limit, result)

    def test_scheduler_is_checked_before_querying_a_batch(self):
        scheduler = mock.Mock()
        syncher = self._syncher(scheduler=scheduler)
        syncher.get_studies = mock.Mock(return_value=[])

        syncher.synch(
            orthanc_source=mock.sentinel.source,
            orthanc_destination=mock.sentinel.destination,
            last_update_limit=datetime.datetime(2026, 7, 30, 12, 0, 0),
        )

        scheduler.wait_right_time_to_run.assert_called_once_with()

    def test_scheduler_pause_restarts_offset_scan(self):
        scheduler = mock.Mock()
        scheduler.wait_right_time_to_run.side_effect = [False, True, False]
        syncher = self._syncher(
            scheduler=scheduler,
            orthanc_queries_batch_size=1,
        )
        syncher.get_studies = mock.Mock(
            side_effect=[
                [
                    SimpleNamespace(
                        last_update=datetime.datetime(2026, 8, 3, 7, 8, 9),
                        orthanc_id="study-id",
                    )
                ],
                [],
            ]
        )
        syncher.compare_studies = mock.Mock()

        syncher.synch(
            orthanc_source=mock.sentinel.source,
            orthanc_destination=mock.sentinel.destination,
            last_update_limit=datetime.datetime(2026, 8, 1, 1, 2, 3),
        )

        self.assertEqual(
            [0, 0],
            [call.kwargs["index"] for call in syncher.get_studies.call_args_list],
        )

    def test_missing_status_file_is_initialized_atomically(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = os.path.join(temp_dir, "status.txt")

            syncher = self._syncher(persist_status_path=status_path)

            self.assertEqual(
                syncher._initial_last_update(),
                syncher._run_till_last_update_1,
            )
            self.assertEqual(
                ["1950-01-01 01:01:01", "1950-01-01 01:01:01"],
                Path(status_path).read_text(encoding="utf-8").splitlines(),
            )
            self.assertEqual(["status.txt"], os.listdir(temp_dir))

    @unittest.skipIf(os.name == "nt", "Windows does not preserve POSIX modes")
    def test_new_status_file_respects_process_umask(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = os.path.join(temp_dir, "status.txt")
            previous_umask = os.umask(0o027)
            try:
                self._syncher(persist_status_path=status_path)
            finally:
                os.umask(previous_umask)

            self.assertEqual(0o640, stat.S_IMODE(os.stat(status_path).st_mode))

    @unittest.skipIf(os.name == "nt", "directory fsync is POSIX-specific")
    def test_atomic_status_update_fsyncs_parent_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = Path(temp_dir, "status.txt")
            status_path.write_text(
                "2026-08-01 01:02:03\n2026-08-02 04:05:06\n",
                encoding="utf-8",
            )
            syncher = self._syncher(persist_status_path=os.fspath(status_path))

            with (
                mock.patch("orthanc_tools.orthanc_syncher.os.open", return_value=123),
                mock.patch("orthanc_tools.orthanc_syncher.os.fsync") as fsync,
                mock.patch("orthanc_tools.orthanc_syncher.os.close") as close,
            ):
                syncher.save_last_update_limit(
                    datetime.datetime(2026, 8, 3, 7, 8, 9),
                    0,
                )

            fsync.assert_any_call(123)
            close.assert_called_once_with(123)

    def test_invalid_status_file_is_preserved_and_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = Path(temp_dir, "status.txt")
            status_path.write_text("truncated\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "Invalid LastUpdate"):
                self._syncher(persist_status_path=os.fspath(status_path))

            self.assertEqual("truncated\n", status_path.read_text(encoding="utf-8"))
            self.assertEqual(["status.txt"], os.listdir(temp_dir))

    @unittest.skipIf(os.name == "nt", "Windows does not preserve POSIX modes")
    def test_status_update_preserves_file_mode(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = Path(temp_dir, "status.txt")
            status_path.write_text(
                "2026-08-01 01:02:03\n2026-08-02 04:05:06\n",
                encoding="utf-8",
            )
            os.chmod(status_path, 0o640)
            syncher = self._syncher(persist_status_path=os.fspath(status_path))

            syncher.save_last_update_limit(
                datetime.datetime(2026, 8, 3, 7, 8, 9),
                0,
            )

            self.assertEqual(0o640, stat.S_IMODE(os.stat(status_path).st_mode))

    @unittest.skipUnless(hasattr(os, "chown"), "ownership changes are unavailable")
    def test_status_update_falls_back_when_owner_cannot_be_assumed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = Path(temp_dir, "status.txt")
            status_path.write_text(
                "2026-08-01 01:02:03\n2026-08-02 04:05:06\n",
                encoding="utf-8",
            )
            syncher = self._syncher(persist_status_path=os.fspath(status_path))

            with mock.patch(
                "orthanc_tools.orthanc_syncher.os.chown",
                side_effect=PermissionError("foreign owner"),
            ):
                syncher.save_last_update_limit(
                    datetime.datetime(2026, 8, 3, 7, 8, 9),
                    0,
                )

            self.assertEqual(
                ["2026-08-03 07:08:09", "2026-08-02 04:05:06"],
                status_path.read_text(encoding="utf-8").splitlines(),
            )
            self.assertEqual(["status.txt"], os.listdir(temp_dir))

    def test_status_update_preserves_explicit_symlink(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_target = Path(temp_dir, "target.txt")
            status_link = Path(temp_dir, "status.txt")
            status_target.write_text(
                "2026-08-01 01:02:03\n2026-08-02 04:05:06\n",
                encoding="utf-8",
            )
            try:
                os.symlink(status_target, status_link)
            except (OSError, NotImplementedError) as ex:
                self.skipTest(f"symbolic links are unavailable: {ex}")
            syncher = self._syncher(persist_status_path=os.fspath(status_link))

            syncher.save_last_update_limit(
                datetime.datetime(2026, 8, 3, 7, 8, 9),
                0,
            )

            self.assertTrue(status_link.is_symlink())
            self.assertEqual(
                ["2026-08-03 07:08:09", "2026-08-02 04:05:06"],
                status_target.read_text(encoding="utf-8").splitlines(),
            )

    def test_busy_checkpoint_falls_back_to_in_place_update(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = Path(temp_dir, "status.txt")
            status_path.write_text(
                "2026-08-01 01:02:03\n2026-08-02 04:05:06\n",
                encoding="utf-8",
            )
            syncher = self._syncher(persist_status_path=os.fspath(status_path))

            with mock.patch(
                "orthanc_tools.orthanc_syncher.os.replace",
                side_effect=OSError(errno.EBUSY, "bind-mounted file"),
            ):
                syncher.save_last_update_limit(
                    datetime.datetime(2026, 8, 3, 7, 8, 9),
                    0,
                )

            self.assertEqual(
                ["2026-08-03 07:08:09", "2026-08-02 04:05:06"],
                status_path.read_text(encoding="utf-8").splitlines(),
            )
            self.assertEqual(["status.txt"], os.listdir(temp_dir))

    def test_read_only_parent_falls_back_to_in_place_update(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = Path(temp_dir, "status.txt")
            status_path.write_text(
                "2026-08-01 01:02:03\n2026-08-02 04:05:06\n",
                encoding="utf-8",
            )
            syncher = self._syncher(persist_status_path=os.fspath(status_path))
            real_open = open

            def open_with_read_only_parent(path, mode="r", *args, **kwargs):
                if mode == "x":
                    raise OSError(errno.EROFS, "read-only parent")
                return real_open(path, mode, *args, **kwargs)

            with mock.patch(
                "orthanc_tools.orthanc_syncher.open",
                side_effect=open_with_read_only_parent,
                create=True,
            ):
                syncher.save_last_update_limit(
                    datetime.datetime(2026, 8, 3, 7, 8, 9),
                    0,
                )

            self.assertEqual(
                ["2026-08-03 07:08:09", "2026-08-02 04:05:06"],
                status_path.read_text(encoding="utf-8").splitlines(),
            )

    def test_hard_linked_checkpoint_updates_all_links(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = Path(temp_dir, "status.txt")
            linked_path = Path(temp_dir, "linked.txt")
            status_path.write_text(
                "2026-08-01 01:02:03\n2026-08-02 04:05:06\n",
                encoding="utf-8",
            )
            try:
                os.link(status_path, linked_path)
            except (OSError, NotImplementedError) as ex:
                self.skipTest(f"hard links are unavailable: {ex}")
            syncher = self._syncher(persist_status_path=os.fspath(status_path))

            syncher.save_last_update_limit(
                datetime.datetime(2026, 8, 3, 7, 8, 9),
                0,
            )

            self.assertEqual(
                ["2026-08-03 07:08:09", "2026-08-02 04:05:06"],
                linked_path.read_text(encoding="utf-8").splitlines(),
            )
            self.assertEqual(os.stat(status_path).st_ino, os.stat(linked_path).st_ino)

    @unittest.skipUnless(
        all(hasattr(os, name) for name in ("listxattr", "getxattr", "setxattr")),
        "extended attributes are unavailable",
    )
    def test_unsupported_checkpoint_xattrs_do_not_block_update(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = Path(temp_dir, "status.txt")
            status_path.write_text(
                "2026-08-01 01:02:03\n2026-08-02 04:05:06\n",
                encoding="utf-8",
            )
            syncher = self._syncher(persist_status_path=os.fspath(status_path))

            with mock.patch(
                "orthanc_tools.orthanc_syncher.os.listxattr",
                side_effect=OSError(errno.ENOTSUP, "xattrs unsupported"),
            ):
                syncher.save_last_update_limit(
                    datetime.datetime(2026, 8, 3, 7, 8, 9),
                    0,
                )

            self.assertEqual(
                ["2026-08-03 07:08:09", "2026-08-02 04:05:06"],
                status_path.read_text(encoding="utf-8").splitlines(),
            )

    def test_empty_transfer_is_a_no_op(self):
        source = mock.MagicMock()
        destination = mock.MagicMock()

        self._syncher().transfer_instances(source, destination, [])

        self.assertEqual([], source.mock_calls)
        self.assertEqual([], destination.mock_calls)

    def test_transfer_failure_raises_instead_of_exiting_process(self):
        source = mock.MagicMock()
        source.instances.get_file.side_effect = RuntimeError("offline")
        source.instances.get_parent_study_id.return_value = "study-id"
        syncher = self._syncher()

        with mock.patch("orthanc_tools.orthanc_syncher.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "6 attempts"):
                syncher.transfer_instances(
                    orthanc_source=source,
                    orthanc_destination=mock.MagicMock(),
                    instances_ids=["instance-id"],
                )

        self.assertEqual(6, source.instances.get_file.call_count)


if __name__ == "__main__":
    unittest.main()
