import math

from orthanc_api_client import OrthancApiClient
from urllib3.util import Retry


class BoundedOrthancApiClient(OrthancApiClient):
    """Bound tool requests and avoid replaying POSTs after uncertain outcomes."""

    def __init__(self, *args, connect_timeout=5, read_timeout=15, **kwargs):
        if not all(math.isfinite(value) and value > 0 for value in (connect_timeout, read_timeout)):
            raise ValueError("HTTP timeouts must be positive")
        super().__init__(*args, **kwargs)
        self._request_timeout = (connect_timeout, read_timeout)
        # The pinned API client exposes transport only through its session.
        # Connection retries are safe before delivery; POST read/status retries
        # can repeat an already accepted job or worklist creation.
        adapter = self._http_session.get_adapter(self._root_url)
        adapter.max_retries = Retry(
            connect=3,
            read=3,
            status=3,
            allowed_methods=Retry.DEFAULT_ALLOWED_METHODS,
            status_forcelist={502, 503},
            backoff_factor=0.2,
        )

    def close(self):
        # The upstream destructor also runs when construction raises.
        session = getattr(self, "_http_session", None)
        if session is not None:
            session.close()

    def recycle_idle_connections(self):
        """Recycle pools between passes, after all forwarding workers join."""
        self._http_session.close()

    def get(self, endpoint, **kwargs):
        kwargs.setdefault("timeout", self._request_timeout)
        return super().get(endpoint, **kwargs)

    def post(self, endpoint, **kwargs):
        kwargs.setdefault("timeout", self._request_timeout)
        return super().post(endpoint, **kwargs)

    def put(self, endpoint, **kwargs):
        kwargs.setdefault("timeout", self._request_timeout)
        return super().put(endpoint, **kwargs)

    def delete(self, endpoint, **kwargs):
        kwargs.setdefault("timeout", self._request_timeout)
        return super().delete(endpoint, **kwargs)
