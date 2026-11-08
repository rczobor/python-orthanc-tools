import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from orthanc_api_client import ChangeType, InstancesSet, OrthancApiClient, ResourceNotFound
from orthanc_tools.orthanc_forwarder import (
    FORWARDING_METADATA_NAME, HEARTBEAT_INTERVAL_SECONDS, TERMINAL_METADATA_NAME,
    ForwarderDestination, ForwarderMode, OrthancForwarder,
)


class TestForwarderJobs(unittest.TestCase):
    def setUp(self):
        self.source = OrthancApiClient("http://unused.invalid/")
        self.metadata = {}
        self.submissions = []
        self.deleted = []
        self.source.get = Mock(side_effect=self.read_metadata)
        self.source.put = Mock(side_effect=self.write_metadata)
        self.source.post = Mock(side_effect=self.post)
        self.source.studies.get = Mock(return_value=SimpleNamespace(
            last_update="20261012T120000", main_dicom_tags={"StudyDescription": "MR BRAIN"},
        ))
        self.source.series.get = Mock(return_value=SimpleNamespace(
            statistics=SimpleNamespace(uncompressed_size=2 * 1024 ** 3),
        ))
        self.source.jobs.get_json = Mock(side_effect=lambda job_id: {
            "ID": job_id, "State": "Success", "Type": "PushTransfer",
        })
        self.snapshot = InstancesSet(self.source)
        self.snapshot.study_id = "study-orthanc-id"
        self.snapshot._add_series("series-orthanc-id", ["instance-1", "instance-2"])

    def read_metadata(self, endpoint, **kwargs):
        if endpoint not in self.metadata:
            raise ResourceNotFound(404)
        return SimpleNamespace(content=self.metadata[endpoint], headers={})

    def write_metadata(self, endpoint, data, **kwargs):
        self.metadata[endpoint] = data

    def post(self, endpoint, json, **kwargs):
        if endpoint == "tools/bulk-delete":
            self.deleted.extend(json["Resources"])
        else:
            self.submissions.append(endpoint)
        return Mock(status_code=200, json=lambda: {"ID": f"job-{len(self.submissions)}"})

    def forwarder(self, mode=ForwarderMode.TRANSFER, **kwargs):
        return OrthancForwarder(self.source, [ForwarderDestination("archive", mode)], **kwargs)

    def test_failed_transfer_retains_source_and_schedules_retry(self):
        self.source.jobs.get_json.side_effect = lambda job_id: {"ID": job_id, "State": "Failure"}
        forwarder = self.forwarder()
        forwarder.handle_instances_set(self.snapshot)
        self.assertEqual([], self.deleted)
        self.assertEqual(1, forwarder._status[self.snapshot.id].retry_count)
        self.assertEqual({}, forwarder._status[self.snapshot.id].pending_jobs)

    def test_all_six_modes_require_success_before_deleting_snapshot(self):
        for mode in ForwarderMode:
            with self.subTest(mode=mode):
                self.metadata.clear()
                self.submissions.clear()
                self.deleted.clear()
                forwarder = self.forwarder(mode)
                forwarder.handle_instances_set(self.snapshot)
                self.assertEqual(self.snapshot.instances_ids, self.deleted)
                self.assertTrue(self.submissions)
                record = json.loads(self.metadata[f"studies/study-orthanc-id/metadata/{FORWARDING_METADATA_NAME}"])
                self.assertEqual([forwarder._destinations[0].retry_key], record["sent"])
                self.assertEqual({}, record["jobs"])

    def test_pending_job_is_resumed_after_timeout_and_restart(self):
        self.source.jobs.get_json.side_effect = lambda job_id: {"ID": job_id, "State": "Running"}
        first = self.forwarder(forward_timeout_in_seconds=0.001)
        first.handle_instances_set(self.snapshot)
        self.assertEqual([], self.deleted)
        self.assertEqual(1, len(self.submissions))
        self.assertTrue(first._status[self.snapshot.id].pending_jobs)
        self.source.jobs.get_json.side_effect = lambda job_id: {"ID": job_id, "State": "Success"}
        self.forwarder().handle_instances_set(self.snapshot)
        self.assertEqual(1, len(self.submissions))
        self.assertEqual(self.snapshot.instances_ids, self.deleted)

    def test_restart_does_not_resend_a_completed_destination(self):
        destinations = [ForwarderDestination("first", ForwarderMode.PEERING),
                        ForwarderDestination("second", ForwarderMode.DICOM)]
        self.source.jobs.get_json.side_effect = lambda job_id: {
            "ID": job_id, "State": "Failure" if job_id == "job-2" else "Success",
        }
        OrthancForwarder(self.source, destinations).handle_instances_set(self.snapshot)
        self.assertEqual([], self.deleted)
        OrthancForwarder(self.source, destinations).handle_instances_set(self.snapshot)
        self.assertEqual(["peers/first/store", "modalities/second/store", "modalities/second/store"], self.submissions)
        self.assertEqual(self.snapshot.instances_ids, self.deleted)

    def test_completed_series_survive_job_history_pruning(self):
        snapshot = InstancesSet(self.source)
        snapshot.study_id = self.snapshot.study_id
        for index in range(12):
            snapshot._add_series(f"series-{index}", [f"instance-{index}"])
        self.source.jobs.get_json.side_effect = lambda job_id: {
            "ID": job_id, "State": "Failure" if job_id == "job-12" else "Success",
        }
        first = self.forwarder(ForwarderMode.DICOM_SERIES_BY_SERIES)
        first.handle_instances_set(snapshot)
        self.assertEqual([], self.deleted)

        def remaining_history(job_id):
            if int(job_id.split("-")[1]) < 4:
                raise ResourceNotFound(404)
            return {"ID": job_id, "State": "Success"}

        self.source.jobs.get_json.side_effect = remaining_history
        self.forwarder(ForwarderMode.DICOM_SERIES_BY_SERIES).handle_instances_set(snapshot)
        self.assertEqual(13, len(self.submissions))
        self.assertEqual(snapshot.instances_ids, self.deleted)

    def test_uncertain_submission_is_not_replayed_after_restart(self):
        self.source.post.side_effect = TimeoutError("response lost after job accepted")
        self.forwarder().handle_instances_set(self.snapshot)
        self.assertEqual(1, self.source.post.call_count)
        self.source.post.reset_mock()
        self.source.post.side_effect = self.post
        self.forwarder().handle_instances_set(self.snapshot)
        self.source.post.assert_not_called()
        self.assertEqual([], self.deleted)

    def test_failed_intent_save_does_not_submit_a_job(self):
        self.source.put.side_effect = RuntimeError("metadata unavailable")
        self.forwarder().handle_instances_set(self.snapshot)
        self.source.post.assert_not_called()
        self.assertEqual([], self.deleted)

    def test_direct_forward_call_keeps_its_public_contract(self):
        sent, eligible = self.forwarder().forward(self.snapshot, [])
        self.assertEqual(sent, eligible)
        self.assertEqual(1, len(sent))
        self.assertEqual([], self.deleted)

    def test_heartbeat_write_failure_does_not_kill_workers(self):
        self.source.studies.get_all_ids = Mock(return_value=["study"] * 10)
        forwarder = self.forwarder()
        forwarder._heartbeat_file = "/heartbeat"
        forwarder._handle_study = Mock()
        with patch("orthanc_tools.orthanc_forwarder.Path.write_text", side_effect=OSError("read-only file")):
            forwarder.handle_all_content()
        self.assertEqual(10, forwarder._handle_study.call_count)
        self.assertEqual(0, forwarder._resources_to_process.unfinished_tasks)

    def test_metadata_uses_real_resource_ids_for_every_trigger(self):
        for trigger, segment, resource_id in (
            (ChangeType.STABLE_STUDY, "studies", self.snapshot.study_id),
            (ChangeType.STABLE_SERIES, "series", self.snapshot.series_ids[0]),
            (ChangeType.NEW_INSTANCE, "instances", self.snapshot.instances_ids[0]),
        ):
            with self.subTest(trigger=trigger):
                self.metadata.clear()
                forwarder = OrthancForwarder(self.source, [], trigger=trigger)
                forwarder.handle_instances_set(self.snapshot)
                self.assertIn(f"{segment}/{resource_id}/metadata/{TERMINAL_METADATA_NAME}", self.metadata)
                self.assertNotIn(self.snapshot.id, forwarder._status)

    def test_corrupt_receipt_keeps_source_without_submitting_jobs(self):
        self.metadata[f"studies/{self.snapshot.study_id}/metadata/{FORWARDING_METADATA_NAME}"] = b'{"jobs":'
        with self.assertRaisesRegex(ValueError, "keeping source data"):
            self.forwarder().handle_instances_set(self.snapshot)
        self.assertEqual([], self.submissions)
        self.assertEqual([], self.deleted)

    def test_source_listing_failure_starts_no_workers(self):
        self.source.studies.get_all_ids = Mock(side_effect=RuntimeError("source unavailable"))
        forwarder = self.forwarder()
        with self.assertRaisesRegex(RuntimeError, "source unavailable"):
            forwarder.handle_all_content()
        self.assertEqual([], forwarder._worker_threads)

    def test_worker_failure_finishes_queue_tasks_and_joins_workers(self):
        self.source.studies.get_all_ids = Mock(return_value=["study-orthanc-id"])
        forwarder = self.forwarder()
        forwarder._handle_study = Mock(side_effect=RuntimeError("processing failed"))
        forwarder.handle_all_content()
        self.assertEqual([], forwarder._worker_threads)
        self.assertEqual(0, forwarder._resources_to_process.unfinished_tasks)

    @staticmethod
    def successful_job(job_id):
        return {"ID": job_id, "State": "Success"}

    def failing_job(self, job_id):
        return {"ID": job_id, "State": "Failure", "ErrorCode": 9,
                "ErrorDescription": "Error in the network protocol",
                "ErrorDetails": "DicomAssociation - connecting to AET \"PBJUMP\":\n  Failed to establish association"}

    def poll_study(self, forwarder, last_update="20261108T120000"):
        self.source.studies.get_all_ids = Mock(return_value=[self.snapshot.study_id])
        self.source.studies.get_json = Mock(return_value={"IsStable": True, "LastUpdate": last_update})
        with patch("orthanc_tools.orthanc_forwarder.InstancesSet.from_study", return_value=self.snapshot) as from_study:
            forwarder.handle_all_content()
        return from_study.call_count

    def test_failed_study_waits_for_retry_without_listing_its_instances(self):
        self.source.jobs.get_json.side_effect = self.failing_job
        forwarder = self.forwarder()
        self.assertEqual(1, self.poll_study(forwarder))
        self.assertEqual(0, self.poll_study(forwarder))
        self.assertEqual(1, len(self.submissions))

        # A changed LastUpdate (new instances) lists the study again.
        self.assertEqual(1, self.poll_study(forwarder, last_update="20261108T120500"))
        self.assertEqual(0, self.poll_study(forwarder, last_update="20261108T120500"))

        forwarder._failures[self.snapshot.study_id].retry_at = 0
        forwarder._status[self.snapshot.id].next_retry = None
        self.source.jobs.get_json.side_effect = self.successful_job
        self.assertEqual(1, self.poll_study(forwarder, last_update="20261108T120500"))
        self.assertEqual(self.snapshot.instances_ids, self.deleted)
        self.assertEqual({}, forwarder._failures)

    def test_unexpected_error_backs_off_instead_of_repeating_every_pass(self):
        self.metadata[f"studies/{self.snapshot.study_id}/metadata/{FORWARDING_METADATA_NAME}"] = b'{"jobs":'
        forwarder = self.forwarder()
        with self.assertLogs("orthanc_tools.orthanc_forwarder", "ERROR"):
            self.assertEqual(1, self.poll_study(forwarder))
        self.assertEqual(0, self.poll_study(forwarder, last_update="20261108T120500"))
        self.assertEqual([], self.submissions)

    def test_delivery_status_turns_failing_after_threshold_and_clears_on_success(self):
        self.source.jobs.get_json.side_effect = self.failing_job
        forwarder = self.forwarder(failure_alert_after_in_seconds=60)
        now = [1000.0]
        with patch("orthanc_tools.orthanc_forwarder.time.monotonic", side_effect=lambda: now[0]):
            self.poll_study(forwarder)
            status = forwarder.delivery_status()
            self.assertEqual(("ok", 1, 0, 1), (status["status"], status["failing_resources"],
                                              status["oldest_failure_seconds"], status["source_resources"]))
            now[0] += 61
            self.assertEqual("failing", forwarder.delivery_status()["status"])
            forwarder._status[self.snapshot.id].next_retry = None
            self.source.jobs.get_json.side_effect = self.successful_job
            self.poll_study(forwarder)
            self.assertEqual(self.snapshot.instances_ids, self.deleted)
            self.assertEqual("ok", forwarder.delivery_status()["status"])

    def test_failure_of_a_resource_that_left_the_source_is_dropped(self):
        self.source.jobs.get_json.side_effect = self.failing_job
        forwarder = self.forwarder(failure_alert_after_in_seconds=1)
        self.poll_study(forwarder)
        self.assertEqual(1, forwarder.delivery_status()["failing_resources"])
        self.source.studies.get_all_ids = Mock(return_value=[])
        forwarder.handle_all_content()
        self.assertEqual(0, forwarder.delivery_status()["failing_resources"])

    def test_alert_disabled_by_default_and_heartbeat_reports_failures(self):
        self.source.jobs.get_json.side_effect = self.failing_job
        forwarder = self.forwarder()
        self.poll_study(forwarder)
        with tempfile.TemporaryDirectory() as folder:
            forwarder._heartbeat_file = str(Path(folder) / "heartbeat")
            forwarder._heartbeat_last_touch = -HEARTBEAT_INTERVAL_SECONDS
            forwarder._heartbeat_progress()
            status = json.loads(Path(forwarder._heartbeat_file).read_text())
        self.assertEqual("ok", status["status"])
        self.assertEqual(1, status["failing_resources"])

    def test_job_failure_logs_orthanc_error_and_names_resource_and_destination(self):
        self.source.jobs.get_json.side_effect = self.failing_job
        with self.assertLogs("orthanc_tools.orthanc_forwarder", "INFO") as logs:
            self.forwarder(ForwarderMode.DICOM).handle_instances_set(self.snapshot)
        error = next(line for line in logs.output if line.startswith("ERROR"))
        self.assertIn("Study study-orthanc-id (1 series / 2 instances) Error while forwarding to archive", error)
        self.assertIn("Error in the network protocol (code 9): DicomAssociation - connecting to AET "
                      "\"PBJUMP\": Failed to establish association", error)

        self.source.jobs.get_json.side_effect = self.successful_job
        with self.assertLogs("orthanc_tools.orthanc_forwarder", "INFO") as logs:
            self.forwarder(ForwarderMode.DICOM).handle_instances_set(self.snapshot)
        self.assertIn("INFO:orthanc_tools.orthanc_forwarder:Study study-orthanc-id (1 series / 2 instances) "
                      "Sent to archive using dicom", logs.output)

    def test_negative_failure_alert_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "failure_alert_after_in_seconds"):
            self.forwarder(failure_alert_after_in_seconds=-1)


if __name__ == "__main__":
    unittest.main()
