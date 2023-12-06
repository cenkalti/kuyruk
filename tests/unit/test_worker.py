import socket
import logging
import unittest

from kuyruk import Kuyruk
from kuyruk import Worker


logger = logging.getLogger(__name__)


class Args:
    def __init__(self, **kwargs):
        self.queues = []
        self.logging_level = None
        self.max_load = None
        self.max_run_time = None
        self.spawn_task_process = False
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
