import io
import os
import pathlib
import time
import unittest

import pydicom
from orthanc_api_client import OrthancApiClient


HERE = pathlib.Path(__file__).parent.resolve()


class TestMpDockerForwarderWorkflow(unittest.TestCase):
    """Exercise the disposable Orthanc stack with a separately running forwarder."""

    @classmethod
    def setUpClass(cls):
        if os.environ.get("ORTHANC_TEST_ALLOW_DELETE") != "1":
            raise unittest.SkipTest("set ORTHANC_TEST_ALLOW_DELETE=1 for the disposable integration stack")

        cls.source = cls._client("SOURCE", "http://localhost:10042")
        cls.always_destination = cls._client("ALWAYS_DESTINATION", "http://localhost:10043")
        cls.filtered_destination = cls._client("FILTERED_DESTINATION", "http://localhost:10044")

        for client in (cls.source, cls.always_destination, cls.filtered_destination):
            client.wait_started()

    @staticmethod
    def _client(name, default_url):
        return OrthancApiClient(
            os.environ.get(f"ORTHANC_{name}_URL", default_url),
            user="test",
            pwd="test",
        )

    def setUp(self):
        for client in (self.source, self.always_destination, self.filtered_destination):
            client.delete_all_content()

    def _upload_study(self, study_description):
        dataset = pydicom.dcmread(HERE / "stimuli" / "CT_small.dcm")
        dataset.StudyDescription = study_description
        payload = io.BytesIO()
        dataset.save_as(payload, enforce_file_format=True)
        return self.source.upload(buffer=payload.getvalue())

    def _wait_for(self, condition, message, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(0.1)
        self.fail(message)

    def test_matching_study_reaches_both_destinations(self):
        instance_ids = self._upload_study("RTG CHEST")

        self._wait_for(
            lambda: (
                not self.source.instances.get_all_ids()
                and len(self.always_destination.instances.get_all_ids()) == len(instance_ids)
                and len(self.filtered_destination.instances.get_all_ids()) == len(instance_ids)
            ),
            "matching study was not forwarded to both destinations",
        )

    def test_non_matching_study_skips_filtered_destination(self):
        instance_ids = self._upload_study("MR BRAIN")

        self._wait_for(
            lambda: (
                not self.source.instances.get_all_ids()
                and len(self.always_destination.instances.get_all_ids()) == len(instance_ids)
            ),
            "non-matching study was not forwarded to the unfiltered destination",
        )
        self.assertEqual([], self.filtered_destination.instances.get_all_ids())


if __name__ == "__main__":
    unittest.main()
