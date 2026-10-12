import io
import json
import os
import pathlib
import socket
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import pydicom
import requests
from orthanc_api_client import InstancesSet

from orthanc_tools import DicomWorklistBuilder, ForwarderDestination, ForwarderMode, OrthancForwarder, OrthancCleaner
from orthanc_tools.helpers.http_client import BoundedOrthancApiClient
from orthanc_tools.orthanc_forwarder import FORWARDING_METADATA_NAME, TERMINAL_METADATA_NAME


HERE = pathlib.Path(__file__).parent


class TestForwarderIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("ORTHANC_TEST_ALLOW_DELETE") != "1":
            raise unittest.SkipTest("disposable integration stack requires explicit opt-in")
        cls.source = cls.client("SOURCE", "http://localhost:10042")
        cls.first = cls.client("ALWAYS_DESTINATION", "http://localhost:10043")
        cls.second = cls.client("FILTERED_DESTINATION", "http://localhost:10044")
        for client in (cls.source, cls.first, cls.second):
            if not client.wait_started(timeout=30):
                raise RuntimeError("disposable Orthanc did not become ready")

    @staticmethod
    def client(name, default):
        return BoundedOrthancApiClient(os.environ.get(f"ORTHANC_{name}_URL", default), user="test", pwd="test")

    def setUp(self):
        for client in (self.source, self.first, self.second):
            client.delete_all_content()

    def upload(self, description="MR BRAIN", study_uid=None):
        dataset = pydicom.dcmread(HERE / "stimuli/CT_small.dcm")
        dataset.PatientName = "Maintenance^Synthetic"
        dataset.PatientID = "maintenance-test"
        dataset.StudyDescription = description
        dataset.StudyInstanceUID = study_uid or pydicom.uid.generate_uid()
        dataset.SeriesInstanceUID = pydicom.uid.generate_uid()
        dataset.SOPInstanceUID = pydicom.uid.generate_uid()
        payload = io.BytesIO()
        dataset.save_as(payload, enforce_file_format=True)
        ids = self.source.upload(payload.getvalue())
        study_id = self.source.instances.get_parent_study_id(ids[0])
        self.wait_for(lambda: self.source.studies.get(study_id).is_stable)
        return study_id, ids, dataset.StudyInstanceUID

    def wait_for(self, condition, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(0.1)
        self.fail("disposable workflow condition timed out")

    def forwarder(self, destinations, **kwargs):
        return OrthancForwarder(self.source, destinations, worker_threads_count=6,
                                polling_interval_in_seconds=0.1, forward_timeout_in_seconds=30, **kwargs)

    def test_unmatched_study_persists_real_metadata_across_restart(self):
        study_id, ids, _ = self.upload()
        destinations = [ForwarderDestination("orthanc-c", ForwarderMode.DICOM,
                                           study_description_match_type="substring", study_description_pattern="RTG")]
        first = self.forwarder(destinations)
        first.handle_all_content()
        marker = self.source.studies.get_string_metadata(study_id, TERMINAL_METADATA_NAME, default_value="")
        self.assertTrue(marker)
        self.forwarder(destinations).handle_all_content()
        self.assertEqual(ids, self.source.instances.get_all_ids())
        self.assertEqual([], self.second.instances.get_all_ids())
        changed = [ForwarderDestination("orthanc-c", ForwarderMode.DICOM,
                                       study_description_match_type="substring", study_description_pattern="MR")]
        self.forwarder(changed).handle_all_content()
        self.assertEqual([], self.source.instances.get_all_ids())
        self.assertEqual(ids, self.second.instances.get_all_ids())

    def test_new_instances_invalidate_terminal_marker(self):
        study_id, ids, study_uid = self.upload()
        destinations = [ForwarderDestination("orthanc-c", ForwarderMode.DICOM,
                                           study_description_match_type="substring", study_description_pattern="RTG")]
        forwarder = self.forwarder(destinations)
        forwarder.handle_all_content()
        before = self.source.studies.get_string_metadata(study_id, TERMINAL_METADATA_NAME)
        time.sleep(1.1)
        self.upload(study_uid=study_uid)
        forwarder.handle_all_content()
        after = self.source.studies.get_string_metadata(study_id, TERMINAL_METADATA_NAME)
        self.assertNotEqual(before, after)
        self.assertEqual(2, len(self.source.instances.get_all_ids()))

    def test_failed_peer_retains_source_and_recovers_after_restart(self):
        self.source.put("peers/maintenance-destination", json=["http://127.0.0.1:9", "test", "test"])
        study_id, ids, _ = self.upload()
        destinations = [ForwarderDestination("orthanc-b", ForwarderMode.PEERING),
                        ForwarderDestination("maintenance-destination", ForwarderMode.PEERING)]
        forwarder = self.forwarder(destinations)
        forwarder.handle_all_content()
        self.assertEqual(ids, self.source.instances.get_all_ids())
        self.assertEqual(ids, self.first.instances.get_all_ids())
        record = json.loads(self.source.studies.get_string_metadata(study_id, FORWARDING_METADATA_NAME))
        self.assertEqual([destinations[0].retry_key], record["sent"])
        self.source.put("peers/maintenance-destination", json=[self.second._root_url, "test", "test"])
        self.forwarder(destinations).handle_all_content()
        self.assertEqual([], self.source.instances.get_all_ids())
        self.assertEqual(ids, self.second.instances.get_all_ids())

    def test_failed_transfer_job_does_not_delete_source(self):
        self.source.put("peers/maintenance-transfer", json=["http://127.0.0.1:9", "test", "test"])
        _, ids, _ = self.upload()
        forwarder = self.forwarder([ForwarderDestination("maintenance-transfer", ForwarderMode.TRANSFER)])
        forwarder.handle_all_content()
        self.assertEqual(ids, self.source.instances.get_all_ids())
        status = next(iter(forwarder._status.values()))
        self.assertEqual(1, status.retry_count)
        self.assertEqual({}, status.pending_jobs)

    def slow_peer(self):
        entered = threading.Event()
        release = threading.Event()
        calls = []
        destination = self.first._root_url

        class Handler(BaseHTTPRequestHandler):
            def do_POST(handler):
                body = handler.rfile.read(int(handler.headers["Content-Length"]))
                calls.append(1)
                entered.set()
                release.wait(10)
                response = requests.post(destination.rstrip("/") + "/instances", data=body,
                                         headers={"Content-Type": handler.headers.get("Content-Type", "application/dicom")},
                                         auth=("test", "test"), timeout=15)
                handler.send_response(response.status_code)
                handler.send_header("Content-Length", str(len(response.content)))
                handler.end_headers()
                handler.wfile.write(response.content)

            def log_message(handler, format, *args):
                pass

        server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(release.set)
        host = socket.gethostbyname(socket.gethostname())
        self.source.put("peers/maintenance-slow", json=[f"http://{host}:{server.server_port}", "test", "test"])
        return entered, release, calls

    def test_live_job_keeps_heartbeat_responsive_and_finishes(self):
        entered, release, calls = self.slow_peer()
        _, ids, _ = self.upload()
        forwarder = self.forwarder([ForwarderDestination("maintenance-slow", ForwarderMode.PEERING)])
        with tempfile.TemporaryDirectory() as folder:
            heartbeat = pathlib.Path(folder) / "heartbeat"
            forwarder._heartbeat_file = str(heartbeat)
            with patch("orthanc_tools.orthanc_forwarder.HEARTBEAT_INTERVAL_SECONDS", 0.05):
                worker = threading.Thread(target=forwarder.handle_all_content)
                worker.start()
                try:
                    self.assertTrue(entered.wait(10))
                    before = heartbeat.stat().st_mtime_ns
                    self.wait_for(lambda: heartbeat.stat().st_mtime_ns > before)
                    self.assertTrue(worker.is_alive())
                    self.assertEqual(ids, self.source.instances.get_all_ids())
                finally:
                    release.set()
                    worker.join(30)
                self.assertFalse(worker.is_alive())
        self.assertEqual(1, len(calls))
        self.assertEqual([], self.source.instances.get_all_ids())

    def test_job_timeout_resumes_same_job_after_restart(self):
        entered, release, calls = self.slow_peer()
        study_id, ids, _ = self.upload()
        destinations = [ForwarderDestination("maintenance-slow", ForwarderMode.PEERING)]
        forwarder = OrthancForwarder(self.source, destinations, forward_timeout_in_seconds=0.3)
        forwarder.handle_all_content()
        self.assertTrue(entered.is_set())
        self.assertEqual(ids, self.source.instances.get_all_ids())
        record = json.loads(self.source.studies.get_string_metadata(study_id, FORWARDING_METADATA_NAME))
        self.assertTrue(record["jobs"])
        release.set()
        self.forwarder(destinations).handle_all_content()
        self.assertEqual(1, len(calls))
        self.assertEqual([], self.source.instances.get_all_ids())

    def test_source_connection_failure_leaves_no_workers_or_heartbeat(self):
        source = BoundedOrthancApiClient("http://127.0.0.1:1", connect_timeout=0.1, read_timeout=0.1)
        forwarder = OrthancForwarder(source, [])
        with tempfile.TemporaryDirectory() as folder:
            heartbeat = pathlib.Path(folder) / "heartbeat"
            forwarder._heartbeat_file = str(heartbeat)
            with self.assertRaises(Exception):
                forwarder.handle_all_content()
            self.assertFalse(heartbeat.exists())
        self.assertEqual([], forwarder._worker_threads)

    def test_real_worklist_replacement_preserves_old_on_invalid_update(self):
        self.source.worklists.delete_all()

        builder = DicomWorklistBuilder(orthanc_client=self.source)
        values = {"AccessionNumber": "maintenance-order", "PatientID": "maintenance-test",
                  "PatientName": "Maintenance^Synthetic", "PatientBirthDate": "20000101", "PatientSex": "O",
                  "RequestedProcedureID": "maintenance-order", "SpecificCharacterSet": "ISO_IR 100",
                  "ScheduledStationAETitle": "TEST", "ScheduledProcedureStepID": "maintenance-step",
                  "ScheduledProcedureStepStartDate": "20261012", "ScheduledProcedureStepStartTime": "120000"}
        old_id = builder.generate(dict(values))
        with self.assertRaises(Exception):
            builder.generate({"AccessionNumber": values["AccessionNumber"]}, avoid_duplicates=True)
        self.assertIn(old_id, [item["ID"] for item in self.source.worklists.get_all()])
        new_id = builder.generate(dict(values), avoid_duplicates=True)
        self.assertEqual([new_id], [item["ID"] for item in self.source.worklists.get_all()])
        self.source.worklists.delete_all()

    def test_cleaner_keep_rule_excludes_labeled_study_on_real_backend(self):
        study_id, ids, _ = self.upload()
        self.source.studies.add_label(study_id, "TO_KEEP")
        with tempfile.TemporaryDirectory() as folder:
            rules = pathlib.Path(folder) / "rules.csv"
            # A future cutoff tests retention without changing Orthanc's
            # server-managed LastUpdate or waiting several weeks.
            rules.write_text("TO_KEEP,-6,,,keep\n")
            cleaner = OrthancCleaner(self.source, None, rules)
            cleaner.execute()
            self.assertEqual(ids, self.source.instances.get_all_ids())
            self.source.studies.delete_label(study_id, "TO_KEEP")
            cleaner.execute()
            self.assertEqual([], self.source.instances.get_all_ids())


if __name__ == "__main__":
    unittest.main()
