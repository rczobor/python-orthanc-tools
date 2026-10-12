import unittest
from unittest.mock import Mock

from requests import Response
from urllib3.exceptions import ProtocolError

from orthanc_tools.helpers.http_client import BoundedOrthancApiClient
from orthanc_tools import OrthancForwarder


class TestBoundedHttpClient(unittest.TestCase):
    def test_requests_have_default_timeouts_and_respect_explicit_overrides(self):
        client = BoundedOrthancApiClient("http://unused.invalid/", connect_timeout=2, read_timeout=7)
        response = Response()
        response.status_code = 200
        client._http_session.request = Mock(return_value=response)
        for method in (client.get, client.post, client.put, client.delete):
            method("system")
            self.assertEqual((2, 7), client._http_session.request.call_args.kwargs["timeout"])
        client.get("system", timeout=1)
        self.assertEqual(1, client._http_session.request.call_args.kwargs["timeout"])

    def test_post_read_failure_is_not_automatically_replayed(self):
        client = BoundedOrthancApiClient("http://unused.invalid/")
        retries = client._http_session.get_adapter("http://unused.invalid/").max_retries
        error = ProtocolError("remote closed connection after request")
        with self.assertRaises(ProtocolError):
            retries.increment(method="POST", url="/transfers/send", error=error)
        self.assertEqual(2, retries.increment(method="GET", url="/studies", error=error).read)

    def test_positional_pool_configuration_is_preserved(self):
        client = BoundedOrthancApiClient("http://unused.invalid/", None, None, None, None, None, 23, True)
        adapter = client._http_session.get_adapter("http://unused.invalid/")
        self.assertEqual(23, adapter._pool_maxsize)
        self.assertTrue(adapter._pool_block)

    def test_nonfinite_or_nonpositive_limits_are_rejected(self):
        for value in (0, -1, float("nan"), float("inf")):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    BoundedOrthancApiClient("http://unused.invalid/", read_timeout=value)
                with self.assertRaises(ValueError):
                    OrthancForwarder(Mock(), [], forward_timeout_in_seconds=value)


if __name__ == "__main__":
    unittest.main()
