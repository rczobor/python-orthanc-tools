import logging
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from orthanc_tools.helpers.logging_config import configure_logging


class TestLoggingConfiguration(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("urllib3.connectionpool")
        self.previous_level = self.logger.level
        self.addCleanup(self.logger.setLevel, self.previous_level)

    def test_cli_defaults_hide_retries_but_preserve_application_logs(self):
        environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        environment.pop("URLLIB3_LOG_LEVEL", None)
        result = subprocess.run([
            sys.executable, "-c",
            "import logging; from orthanc_tools.helpers.logging_config import configure_logging; "
            "configure_logging(logging.DEBUG); "
            "logging.getLogger('urllib3.connectionpool').warning('retry noise'); "
            "logging.getLogger('urllib3.connectionpool').error('transport failure'); "
            "logging.getLogger('orthanc_tools').info('forwarded study'); "
            "logging.getLogger('orthanc_tools').debug('forwarder detail')",
        ], check=True, capture_output=True, text=True, env=environment)
        self.assertNotIn("retry noise", result.stderr)
        self.assertIn("transport failure", result.stderr)
        self.assertIn("forwarded study", result.stderr)
        self.assertIn("forwarder detail", result.stderr)

    def test_explicit_diagnostic_override(self):
        with patch.dict(os.environ, {"URLLIB3_LOG_LEVEL": "debug"}), patch("logging.basicConfig"):
            configure_logging()
        self.assertTrue(self.logger.isEnabledFor(logging.DEBUG))

    def test_invalid_override_is_rejected(self):
        with patch.dict(os.environ, {"URLLIB3_LOG_LEVEL": "quiet"}), patch("logging.basicConfig") as basic:
            with self.assertRaisesRegex(ValueError, "URLLIB3_LOG_LEVEL"):
                configure_logging()
        basic.assert_not_called()

    def test_library_import_does_not_change_logging(self):
        subprocess.run([
            sys.executable, "-c",
            "import logging; logger = logging.getLogger('urllib3.connectionpool'); "
            "logger.setLevel(logging.WARNING); import orthanc_tools; "
            "assert logger.level == logging.WARNING; assert not logging.getLogger().handlers",
        ], check=True, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})


if __name__ == "__main__":
    unittest.main()
