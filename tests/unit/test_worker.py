import errno
import signal
import socket
import logging
import unittest
import multiprocessing
from unittest.mock import MagicMock, patch

import amqp

from kuyruk import Kuyruk
from kuyruk import Worker
from kuyruk import signals
from kuyruk.connection import SingleConnection
from kuyruk.exceptions import HeartbeatError


logger = logging.getLogger(__name__)


class Args:
    def __init__(self, **kwargs):
        self.queues = []
        self.logging_level = None
        self.max_load = None
        self.max_run_time = None
        self.priority = None
        for k, v in kwargs.items():
            setattr(self, k, v)


def _fake_connection(heartbeat_tick=None):
    conn = MagicMock(spec=amqp.Connection)
    ch = conn.channel.return_value
    ch.connection = conn
    conn.heartbeat_tick.side_effect = heartbeat_tick
    return conn


class WorkerTestCase(unittest.TestCase):

    def test_default_queue(self):
        """Consume from "kuyruk" if no queue is given"""
        k = Kuyruk()
        w = Worker(k, Args())
        self.assertListEqual(w.queues, ['kuyruk'])

    def test_queue_names(self):
        """Hostname is appended to local queues"""
        given = ['foo', 'bar.localhost']
        k = Kuyruk()
        w = Worker(k, Args(queues=given))

        hostname = socket.gethostname()
        expected = ['foo', 'bar.%s' % hostname]

        self.assertListEqual(w.queues, expected)

    def test_max_load_minus_one(self):
        """--max-load -1 resolves to the number of CPUs"""
        k = Kuyruk()
        w = Worker(k, Args(max_load=-1))
        self.assertEqual(w._max_load, multiprocessing.cpu_count())

    def test_sighup_raises_heartbeat_error(self):
        """SIGHUP handler raises HeartbeatError instead of AttributeError"""
        k = Kuyruk()
        w = Worker(k, Args())
        with self.assertRaises(HeartbeatError):
            w._handle_sighup(signal.SIGHUP, None)

    def test_reconnect_when_closing_dead_connection_fails(self):
        """Worker reconnects when closing a dead connection fails with ETIMEDOUT"""
        k = Kuyruk()
        k.config.WORKER_RECONNECT_INTERVAL = 0
        w = Worker(k, Args())

        def missed_heartbeats():
            raise amqp.exceptions.ConnectionForced('Too many heartbeats missed')

        dead = _fake_connection(missed_heartbeats)
        dead.close.side_effect = TimeoutError(errno.ETIMEDOUT, 'Connection timed out')
        healthy = _fake_connection(lambda: w.shutdown_pending.set())

        with patch.object(SingleConnection, 'new_connection', side_effect=[dead, healthy]):
            w.run()

        healthy.close.assert_called_once()

    def test_reconnect_when_server_closes_connection(self):
        """Worker reconnects when the server drops the TCP connection without AMQP close"""
        k = Kuyruk()
        k.config.WORKER_RECONNECT_INTERVAL = 0
        w = Worker(k, Args())

        dropped = _fake_connection()
        dropped.drain_events.side_effect = OSError('Server unexpectedly closed connection')
        healthy = _fake_connection(lambda: w.shutdown_pending.set())

        with patch.object(SingleConnection, 'new_connection', side_effect=[dropped, healthy]):
            w.run()

        healthy.close.assert_called_once()

    def test_import_failure_sends_queue(self):
        """worker_failure is sent with queue when task cannot be imported"""
        k = Kuyruk()
        w = Worker(k, Args())
        message = MagicMock()
        message.delivery_info = {'routing_key': 'foo'}
        received = []

        def on_failure(sender, **kwargs):
            received.append(kwargs)

        signals.worker_failure.connect(on_failure, sender=k)
        try:
            w._process_description(message, {'module': 'no_such_module', 'function': 'f'})
        finally:
            signals.worker_failure.disconnect(on_failure, sender=k)

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]['queue'], 'foo')
        message.channel.basic_reject.assert_called_once_with(message.delivery_tag, requeue=False)
