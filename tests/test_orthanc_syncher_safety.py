import datetime
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from orthanc_tools.orthanc_syncher import OrthancSyncher


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
