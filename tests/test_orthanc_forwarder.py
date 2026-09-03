import os
import tempfile
import unittest
from unittest import mock

from orthanc_api_client import ChangeType, exceptions

from orthanc_tools.orthanc_forwarder import (
    OrthancForwarder,
    ForwarderDestination,
    ForwarderInstancesSetStatus,
    ForwarderMode,
    StudyDescriptionMatchType,
    build_forwarder_destination,
    parse_forwarder_destinations,
    split_cli_destination_entries,
    split_destination_entries,
)


class TestOrthancForwarderConfiguration(unittest.TestCase):

    def test_build_destination_without_filter(self):
        destination = build_forwarder_destination("orthanc-b:dicom", ForwarderMode.TRANSFER)

        self.assertEqual("orthanc-b", destination.destination)
        self.assertEqual(ForwarderMode.DICOM, destination.forwarder_mode)
        self.assertIsNone(destination.study_description_match_type)
        self.assertIsNone(destination.study_description_pattern)

    def test_build_destination_with_substring_filter(self):
        destination = build_forwarder_destination("ai:dicom:substring:brain", ForwarderMode.TRANSFER)

        self.assertEqual("ai", destination.destination)
        self.assertEqual(ForwarderMode.DICOM, destination.forwarder_mode)
        self.assertEqual(StudyDescriptionMatchType.SUBSTRING, destination.study_description_match_type)
        self.assertEqual("brain", destination.study_description_pattern)
        self.assertTrue(destination.matches_study_description("Brain MRI"))
        self.assertFalse(destination.matches_study_description("CT Abdomen"))

    def test_build_destination_with_regex_filter(self):
        destination = build_forwarder_destination("ai:dicom:regex:^brain:mr$", ForwarderMode.TRANSFER)

        self.assertEqual("ai", destination.destination)
        self.assertEqual(ForwarderMode.DICOM, destination.forwarder_mode)
        self.assertEqual(StudyDescriptionMatchType.REGEX, destination.study_description_match_type)
        self.assertEqual("^brain:mr$", destination.study_description_pattern)
        self.assertTrue(destination.matches_study_description("Brain:MR"))
        self.assertFalse(destination.matches_study_description("Spine:MR"))

    def test_build_destination_uses_default_mode_when_left_blank(self):
        destination = build_forwarder_destination("ai::substring:brain", ForwarderMode.TRANSFER)

        self.assertEqual(ForwarderMode.TRANSFER, destination.forwarder_mode)
        self.assertEqual(StudyDescriptionMatchType.SUBSTRING, destination.study_description_match_type)

    def test_build_destination_rejects_invalid_match_type(self):
        with self.assertRaisesRegex(ValueError, "Invalid StudyDescription match type"):
            build_forwarder_destination("ai:dicom:exact:brain", ForwarderMode.TRANSFER)

    def test_build_destination_rejects_blank_pattern(self):
        with self.assertRaisesRegex(ValueError, "pattern is missing"):
            build_forwarder_destination("ai:dicom:substring:", ForwarderMode.TRANSFER)

    def test_build_destination_rejects_invalid_regex(self):
        with self.assertRaisesRegex(ValueError, "Invalid StudyDescription regex"):
            build_forwarder_destination("ai:dicom:regex:[", ForwarderMode.TRANSFER)

    def test_parse_multiple_destinations(self):
        destinations = parse_forwarder_destinations(
            ["orthanc-b:dicom", "ai::substring:brain"],
            ForwarderMode.TRANSFER
        )

        self.assertEqual(2, len(destinations))
        self.assertEqual(ForwarderMode.DICOM, destinations[0].forwarder_mode)
        self.assertEqual(ForwarderMode.TRANSFER, destinations[1].forwarder_mode)

    def test_forwarder_rejects_duplicate_destination_retry_keys(self):
        duplicate_destinations = [
            ForwarderDestination(destination="orthanc-b", forwarder_mode=ForwarderMode.DICOM),
            ForwarderDestination(destination="orthanc-b", forwarder_mode=ForwarderMode.DICOM),
        ]

        with self.assertRaisesRegex(ValueError, "Duplicate forwarder destinations"):
            OrthancForwarder(
                source=mock.MagicMock(),
                destinations=duplicate_destinations,
            )

    def test_retry_key_includes_filter(self):
        unfiltered = ForwarderDestination(destination="ai", forwarder_mode=ForwarderMode.DICOM)
        filtered = ForwarderDestination(
            destination="ai",
            forwarder_mode=ForwarderMode.DICOM,
            study_description_match_type=StudyDescriptionMatchType.SUBSTRING,
            study_description_pattern="brain"
        )

        self.assertNotEqual(unfiltered.retry_key, filtered.retry_key)

    def test_split_destination_entries_preserves_quoted_commas(self):
        destinations = split_destination_entries('orthanc-b:dicom,"ai:dicom:substring:CT, ABDOMEN"')

        self.assertEqual(
            ["orthanc-b:dicom", "ai:dicom:substring:CT, ABDOMEN"],
            destinations
        )

    def test_split_destination_entries_preserves_backslashes_in_regex_patterns(self):
        destinations = split_destination_entries(r'ai:dicom:regex:^CT\d+$')

        self.assertEqual([r'ai:dicom:regex:^CT\d+$'], destinations)

    def test_split_cli_destination_entries_splits_each_argument(self):
        destinations = split_cli_destination_entries([
            'peer-a:peering,modality-b:dicom',
            '"ai:dicom:substring:CT, ABDOMEN"'
        ])

        self.assertEqual(
            ["peer-a:peering", "modality-b:dicom", "ai:dicom:substring:CT, ABDOMEN"],
            destinations
        )

    def test_parse_destination_with_comma_in_pattern(self):
        destinations = parse_forwarder_destinations(
            ["ai:dicom:substring:CT, ABDOMEN"],
            ForwarderMode.TRANSFER
        )

        self.assertEqual(1, len(destinations))
        self.assertEqual("CT, ABDOMEN", destinations[0].study_description_pattern)


class TestOrthancForwarderLifecycle(unittest.TestCase):

    def test_heartbeat_is_refreshed_periodically(self):
        forwarder = OrthancForwarder(
            source=mock.MagicMock(),
            destinations=[],
        )
        stop_event = mock.Mock()
        stop_event.wait.side_effect = [False, True]

        with mock.patch("orthanc_tools.orthanc_forwarder.Path.touch") as touch:
            forwarder._heartbeat_loop("/heartbeat", stop_event)

        self.assertEqual(2, touch.call_count)

    def test_execute_updates_heartbeat_after_successful_pass(self):
        forwarder = OrthancForwarder(
            source=mock.MagicMock(),
            destinations=[],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            heartbeat_path = os.path.join(temp_dir, "forwarder-heartbeat")
            with mock.patch.dict(os.environ, {"HEARTBEAT_FILE": heartbeat_path}):
                with mock.patch.object(forwarder, "wait_orthanc_started"):
                    with mock.patch.object(
                        forwarder,
                        "handle_all_content",
                        side_effect=[None, RuntimeError("stop")],
                    ):
                        with mock.patch("orthanc_tools.orthanc_forwarder.time.sleep"):
                            with self.assertRaisesRegex(RuntimeError, "stop"):
                                forwarder.execute()

            self.assertTrue(os.path.isfile(heartbeat_path))

    def test_source_failure_does_not_leave_worker_threads_running(self):
        source = mock.MagicMock()
        source.studies.get_all_ids.side_effect = RuntimeError("source unavailable")
        forwarder = OrthancForwarder(
            source=source,
            destinations=[],
            trigger=ChangeType.STABLE_STUDY,
        )

        with mock.patch("orthanc_tools.orthanc_forwarder.threading.Thread") as thread:
            with self.assertRaisesRegex(RuntimeError, "source unavailable"):
                forwarder.handle_all_content()

        thread.assert_not_called()


class FakeInstancesSet:
    def __init__(self):
        self.id = "study-1"
        self.instances_ids = ["instance-1"]
        self.series_ids = []
        self.deleted = False
        self.filtered = None
        self.filter_calls = 0
        self.process_calls = 0

    def delete(self):
        self.deleted = True

    def filter_instances(self, instance_filter):
        self.filter_calls += 1
        self.filtered = FakeInstancesSet()
        self.filtered.id = f"{self.id}-filtered"
        self.filtered.instances_ids = []

        for instance_id in self.instances_ids:
            if not instance_filter(mock.sentinel.api_client, instance_id):
                self.filtered.instances_ids.append(instance_id)

        return self.filtered

    def process_instances(self, processor):
        self.process_calls += 1
        for instance_id in self.instances_ids:
            processor(mock.sentinel.api_client, instance_id)


class TestOrthancForwarderFilteringBehavior(unittest.TestCase):

    def test_terminal_marker_uses_the_orthanc_study_id(self):
        source = mock.MagicMock()
        source.studies.get.return_value.last_update = "20260829T120000"
        forwarder = OrthancForwarder(source=source, destinations=[])
        instances_set = FakeInstancesSet()
        instances_set.id = "internal-set-id"
        instances_set.study_id = "orthanc-study-id"

        forwarder._terminal_marker(instances_set)

        source.studies.get.assert_called_once_with("orthanc-study-id")

    def test_processing_failure_is_retried_without_forwarding_or_deleting(self):
        instance_processor = mock.Mock(
            side_effect=exceptions.OrthancApiException("processing failed")
        )
        forwarder = OrthancForwarder(
            source=mock.MagicMock(),
            destinations=[
                ForwarderDestination(
                    destination="orthanc-b",
                    forwarder_mode=ForwarderMode.DICOM,
                )
            ],
            instance_processor=instance_processor,
        )
        instances_set = FakeInstancesSet()

        with mock.patch.object(forwarder, "forward") as forward:
            with mock.patch.object(forwarder, "delete") as delete:
                forwarder.handle_instances_set(instances_set)

        status = forwarder._status[instances_set.id]
        self.assertFalse(status.processed)
        self.assertEqual(1, status.retry_count)
        self.assertIsNotNone(status.next_retry)
        forward.assert_not_called()
        delete.assert_not_called()

    def test_overridden_forward_and_delete_keep_original_signatures(self):
        class CustomForwarder(OrthancForwarder):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.deleted_instances_set = None

            def forward(self, instances_set, already_sent_to_destinations):
                return ["dicom:orthanc-b::"], ["dicom:orthanc-b::"]

            def delete(self, instances_set):
                self.deleted_instances_set = instances_set

        forwarder = CustomForwarder(
            source=mock.MagicMock(),
            destinations=[ForwarderDestination(destination="orthanc-b", forwarder_mode=ForwarderMode.DICOM)]
        )
        instances_set = FakeInstancesSet()

        forwarder.handle_instances_set(instances_set)

        self.assertIs(instances_set, forwarder.deleted_instances_set)

    def test_forward_does_not_lookup_study_description_when_no_destination_is_filtered(self):
        forwarder = OrthancForwarder(
            source=mock.MagicMock(),
            destinations=[ForwarderDestination(destination="orthanc-b", forwarder_mode=ForwarderMode.DICOM)]
        )
        instances_set = FakeInstancesSet()

        with mock.patch.object(forwarder, "_get_study_description", side_effect=AssertionError("unexpected lookup")):
            with mock.patch.object(forwarder, "_forward_to_destination") as forward_to_destination:
                sent_to_destinations, eligible_destinations = forwarder.forward(instances_set, [])

        self.assertEqual(["dicom:orthanc-b::"], sent_to_destinations)
        self.assertEqual(["dicom:orthanc-b::"], eligible_destinations)
        forward_to_destination.assert_called_once_with(instances_set=instances_set, destination=forwarder._destinations[0])

    def test_source_is_kept_until_every_destination_succeeds(self):
        forwarder = OrthancForwarder(
            source=mock.MagicMock(),
            destinations=[
                ForwarderDestination(destination="orthanc-b", forwarder_mode=ForwarderMode.DICOM),
                ForwarderDestination(destination="orthanc-c", forwarder_mode=ForwarderMode.DICOM),
            ],
        )
        instances_set = FakeInstancesSet()
        attempts = []

        def forward_to_destination(instances_set, destination):
            attempts.append(destination.destination)
            if destination.destination == "orthanc-c" and attempts.count("orthanc-c") == 1:
                raise RuntimeError("destination unavailable")

        with mock.patch.object(forwarder, "_forward_to_destination", side_effect=forward_to_destination):
            forwarder.handle_instances_set(instances_set)

            self.assertFalse(instances_set.deleted)
            self.assertEqual(["dicom:orthanc-b::"], forwarder._status[instances_set.id].sent_to_destinations)

            forwarder._status[instances_set.id].next_retry = None
            forwarder.handle_instances_set(instances_set)

        self.assertTrue(instances_set.deleted)
        self.assertEqual(["orthanc-b", "orthanc-c", "orthanc-c"], attempts)

    def test_filtered_only_non_matching_study_runs_hooks_before_terminal_skip(self):
        instance_filter = mock.Mock(return_value=False)
        instance_processor = mock.Mock()
        source = mock.MagicMock()
        metadata = {}
        source.studies.get_string_metadata.side_effect = (
            lambda orthanc_id, metadata_name, default_value: metadata.get((orthanc_id, metadata_name), default_value)
        )
        source.studies.set_string_metadata.side_effect = (
            lambda orthanc_id, metadata_name, content: metadata.__setitem__((orthanc_id, metadata_name), content)
        )
        source.studies.get.return_value.last_update = "20260828T220000"
        forwarder = OrthancForwarder(
            source=source,
            destinations=[
                ForwarderDestination(
                    destination="ai",
                    forwarder_mode=ForwarderMode.DICOM,
                    study_description_match_type=StudyDescriptionMatchType.SUBSTRING,
                    study_description_pattern="brain"
                )
            ],
            instance_filter=instance_filter,
            instance_processor=instance_processor
        )
        instances_set = FakeInstancesSet()

        with mock.patch.object(forwarder, "_get_study_description", return_value="abdomen") as get_study_description:
            with mock.patch.object(forwarder, "_forward_to_destination") as forward_to_destination:
                forwarder.handle_instances_set(instances_set)
                forwarder.handle_instances_set(instances_set)

        self.assertFalse(instances_set.deleted)
        self.assertNotIn(instances_set.id, forwarder._status)
        self.assertEqual(1, instances_set.filter_calls)
        self.assertTrue(instances_set.filtered.deleted)
        self.assertEqual(1, instances_set.process_calls)
        instance_filter.assert_called_once_with(mock.sentinel.api_client, "instance-1")
        instance_processor.assert_called_once_with(mock.sentinel.api_client, "instance-1")
        get_study_description.assert_called_once_with(instances_set)
        forward_to_destination.assert_not_called()

    def test_terminal_status_is_shared_by_new_forwarder_instances(self):
        source = mock.MagicMock()
        metadata = {}
        source.studies.get_string_metadata.side_effect = (
            lambda orthanc_id, metadata_name, default_value: metadata.get((orthanc_id, metadata_name), default_value)
        )
        source.studies.set_string_metadata.side_effect = (
            lambda orthanc_id, metadata_name, content: metadata.__setitem__((orthanc_id, metadata_name), content)
        )
        source.studies.get.return_value.last_update = "20260828T220000"
        first = FakeInstancesSet()
        processor = mock.Mock()
        first_forwarder = OrthancForwarder(source=source, destinations=[], instance_processor=processor)
        second_forwarder = OrthancForwarder(source=source, destinations=[], instance_processor=processor)

        first_forwarder.handle_instances_set(first)
        second_forwarder.handle_instances_set(first)

        self.assertNotIn(first.id, first_forwarder._status)
        self.assertNotIn(first.id, second_forwarder._status)
        self.assertEqual(1, first.process_calls)
        source.studies.set_string_metadata.assert_called_once()

        changed_forwarder = OrthancForwarder(
            source=source,
            destinations=[ForwarderDestination(destination="orthanc-b", forwarder_mode=ForwarderMode.DICOM)],
        )
        self.assertFalse(changed_forwarder._is_terminal(first))

        source.studies.get.return_value.last_update = "20260828T220100"
        updated_forwarder = OrthancForwarder(source=source, destinations=[], instance_processor=processor)
        updated_forwarder.handle_instances_set(first)
        self.assertEqual(2, first.process_calls)
        self.assertEqual(2, source.studies.set_string_metadata.call_count)

    def test_already_sent_study_is_deleted_when_remaining_filtered_destinations_do_not_match(self):
        forwarder = OrthancForwarder(
            source=mock.MagicMock(),
            destinations=[
                ForwarderDestination(destination="orthanc-b", forwarder_mode=ForwarderMode.DICOM),
                ForwarderDestination(
                    destination="ai",
                    forwarder_mode=ForwarderMode.DICOM,
                    study_description_match_type=StudyDescriptionMatchType.SUBSTRING,
                    study_description_pattern="brain"
                )
            ]
        )
        instances_set = FakeInstancesSet()
        forwarder._status[instances_set.id] = ForwarderInstancesSetStatus()
        forwarder._status[instances_set.id].sent_to_destinations = ["dicom:orthanc-b::"]

        with mock.patch.object(forwarder, "_get_study_description", return_value="abdomen") as get_study_description:
            with mock.patch.object(forwarder, "_forward_to_destination") as forward_to_destination:
                forwarder.handle_instances_set(instances_set)

        self.assertTrue(instances_set.deleted)
        self.assertEqual(0, instances_set.process_calls)
        self.assertNotIn(instances_set.id, forwarder._status)
        get_study_description.assert_called_once_with(instances_set)
        forward_to_destination.assert_not_called()

    def test_filter_metadata_read_failure_keeps_filtered_destination_retryable(self):
        forwarder = OrthancForwarder(
            source=mock.MagicMock(),
            destinations=[
                ForwarderDestination(destination="orthanc-b", forwarder_mode=ForwarderMode.DICOM),
                ForwarderDestination(
                    destination="ai",
                    forwarder_mode=ForwarderMode.DICOM,
                    study_description_match_type=StudyDescriptionMatchType.SUBSTRING,
                    study_description_pattern="brain"
                )
            ],
            polling_interval_in_seconds=0
        )
        instances_set = FakeInstancesSet()

        with mock.patch.object(forwarder, "_get_study_description", side_effect=RuntimeError("study lookup failed")):
            with mock.patch.object(forwarder, "_forward_to_destination") as forward_to_destination:
                forwarder.handle_instances_set(instances_set)

        self.assertFalse(instances_set.deleted)
        self.assertEqual(["dicom:orthanc-b::"], forwarder._status[instances_set.id].sent_to_destinations)
        self.assertEqual(1, forwarder._status[instances_set.id].retry_count)
        self.assertIsNotNone(forwarder._status[instances_set.id].next_retry)
        forward_to_destination.assert_called_once_with(instances_set=instances_set, destination=forwarder._destinations[0])
