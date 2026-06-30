import signal
import socket
import logging
import unittest
import multiprocessing

from kuyruk import Kuyruk
from kuyruk import Worker
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
