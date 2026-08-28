import threading
import unittest
from unittest.mock import patch

import pika

from orthanc_tools import OrthancReplicator
from tests import test_orthanc_replicator


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
    def __init__(self, channel):
        self._channel = channel
        self.is_open = True
        self.on_channel = None

    def channel(self):
        if self.on_channel:
            self.on_channel()
        return self._channel

    def add_callback_threadsafe(self, callback):
        callback()

    def close(self):
        self.is_open = False
        self._channel.stopped.set()


class AliveOrthanc:
    def is_alive(self):
        return True


class TestOrthancReplicatorLifecycle(unittest.TestCase):
    def test_readiness_probe_has_bounded_connection_parameters(self):
        with patch(
            "tests.test_orthanc_replicator.pika.BlockingConnection",
            side_effect=pika.exceptions.AMQPConnectionError,
        ) as connect:
            self.assertFalse(test_orthanc_replicator.TestOrthancReplicator.rabbitmq_is_ready())

        params = connect.call_args.args[0]
        self.assertEqual(1, params.connection_attempts)
        self.assertEqual(5, params.socket_timeout)
        self.assertEqual(5, params.stack_timeout)

    def test_connection_setup_uses_bounded_parameters(self):
        broker_params = pika.ConnectionParameters(
            connection_attempts=3,
            socket_timeout=None,
            stack_timeout=None,
            blocked_connection_timeout=None,
        )
        replicator = OrthancReplicator(AliveOrthanc(), AliveOrthanc(), broker_params)

        bounded_params = replicator._bounded_broker_params()

        self.assertEqual(1, bounded_params.connection_attempts)
        self.assertEqual(5, bounded_params.socket_timeout)
        self.assertEqual(5, bounded_params.stack_timeout)
        self.assertEqual(5, bounded_params.blocked_connection_timeout)
        self.assertEqual(3, broker_params.connection_attempts)
        self.assertIsNone(broker_params.socket_timeout)

    def test_stop_waits_for_its_consumer_thread(self):
        channel = FakeChannel()
        connection = FakeConnection(channel)
        replicator = OrthancReplicator(
            AliveOrthanc(),
            AliveOrthanc(),
            pika.ConnectionParameters(),
        )
        connection.on_channel = lambda: self.assertIs(
            connection,
            replicator._connection,
        )

        with patch("orthanc_tools.orthanc_replicator.pika.BlockingConnection", return_value=connection):
            replicator.execute()
            try:
                self.assertTrue(channel.consuming.wait(1))
                replicator.stop()
                self.assertFalse(replicator._consuming_thread.is_alive())
            finally:
                channel.stopped.set()
                replicator._stop_requested = True
                replicator._consuming_thread.join(1)


if __name__ == "__main__":
    unittest.main()
