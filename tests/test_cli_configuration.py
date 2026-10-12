import argparse
import os
import subprocess
import sys
import typing
import unittest
from unittest.mock import patch

from orthanc_tools.helpers.scheduler import Scheduler
from orthanc_tools.hl7Lib.hl7_report_series_builder import ReportSeriesBuilder


class TestCliConfiguration(unittest.TestCase):
    def test_importer_help_constructs_parser_without_connecting(self):
        result = subprocess.run([sys.executable, '-m', 'orthanc_tools.orthanc_folder_importer', '--help'],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--worker_threads_count', result.stdout)

    def test_scheduler_defaults_and_environment_schedule(self):
        parser = argparse.ArgumentParser()
        Scheduler.add_parser_arguments(parser)
        arguments = parser.parse_args([])
        with patch.dict(os.environ, {}, clear=True):
            scheduler = Scheduler.create_from_args_and_env_var(arguments)
        self.assertEqual(str(scheduler._running_periods.timezone), 'Etc/UTC')
        self.assertTrue(scheduler._running_periods.is_in_period())
        with patch.dict(os.environ, {'RUN_SCHEDULE': '{"Monday": ["0-6"]}', 'TZ': 'Europe/Budapest'}, clear=True):
            scheduler = Scheduler.create_from_args_and_env_var(arguments)
        self.assertEqual(len(scheduler._running_periods.periods), 1)
        self.assertEqual(str(scheduler._running_periods.timezone), 'Europe/Budapest')

    def test_report_builder_annotation_can_be_resolved(self):
        self.assertEqual(typing.get_type_hints(ReportSeriesBuilder.__init__)['orthanc_client'].__name__, 'OrthancApiClient')


if __name__ == "__main__":
    unittest.main()
