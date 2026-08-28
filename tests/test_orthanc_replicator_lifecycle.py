import threading
import unittest
from unittest.mock import patch

from orthanc_tools import OrthancReplicator


class FakeChannel:
    def __init__(self):
        self.consuming = threading.Event()
        self.stopped = threading.Event()

    def exchange_declare(self, **kwargs):
        pass

    def queue_declare(self, **kwargs):
        pass

    def queue_bind(self, **kwargs):
        pass

    def basic_consume(self, **kwargs):
        pass

    def basic_publish(self, **kwargs):
        pass

    def start_consuming(self):
        self.consuming.set()
        self.stopped.wait()

    def stop_consuming(self):
        self.stopped.set()


class FakeConnection:
    is_open = True

    def __init__(self, channel):
        self._channel = channel

    def channel(self):
        return self._channel

    def add_callback_threadsafe(self, callback):
        callback()

    def close(self):
        pass


class AliveOrthanc:
    def is_alive(self):
        return True


class TestOrthancReplicatorLifecycle(unittest.TestCase):
    def test_stop_waits_for_its_consumer_thread(self):
        channel = FakeChannel()
        connection = FakeConnection(channel)
        replicator = OrthancReplicator(AliveOrthanc(), AliveOrthanc(), object())

        with patch("orthanc_tools.orthanc_replicator.pika.BlockingConnection", return_value=connection):
            replicator.execute()
            self.assertTrue(channel.consuming.wait(1))
            try:
                replicator.stop()
                self.assertFalse(replicator._consuming_thread.is_alive())
            finally:
                channel.stopped.set()
                replicator._consuming_thread.join(1)


if __name__ == "__main__":
    unittest.main()
