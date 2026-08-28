import unittest
from unittest import mock

from tests import test_label_modifier


class TestAuthorizationReadiness(unittest.TestCase):
    def test_each_request_has_a_bounded_timeout(self):
        response = mock.Mock(status_code=200)

        with mock.patch(
            "tests.test_label_modifier.time.monotonic",
            side_effect=[100, 100],
        ), mock.patch(
            "tests.test_label_modifier.requests.get",
            return_value=response,
        ) as get:
            test_label_modifier.TestLabelModifier.wait_auth_service_started(
                "http://auth",
                ("user", "password"),
                timeout=120,
            )

        get.assert_called_once_with(
            url="http://auth/settings/roles",
            auth=("user", "password"),
            timeout=5,
        )


if __name__ == "__main__":
    unittest.main()
