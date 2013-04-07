import sys
import time
import signal
import logging
import unittest
import threading
from Queue import LifoQueue

from scripttest import TestFileEnvironment

from kuyruk import Kuyruk, Queue, Task
from kuyruk.connection import LazyConnection, LazyChannel
from util import kill_cmd

logger = logging.getLogger(__name__)

kuyruk = Kuyruk()


# These 2 functions below needs to be at module level in order that Kuyruk
# to determine their fully qualified name.
@kuyruk.task
def print_task(message):
    print message


@kuyruk.task(queue='another_queue')
def print_task2(message):
    print message


def run_kuyruk(queues='kuyruk'):
    env = TestFileEnvironment()
    out = LifoQueue()

    def target():
        result = env.run(
            sys.executable,
            '-m', 'kuyruk.__main__',  # run main module
            '--queues', queues,
            expect_stderr=True  # logging output goes to stderr
        )
        out.put(result)
    t = threading.Thread(target=target)
    t.start()
    time.sleep(1)
    kill_cmd('kuyruk.__main__', signum=signal.SIGTERM)
    time.sleep(1)
    return out.get()


class KuyrukTestCase(unittest.TestCase):

    def clear_queue(self, queue_name):
        with LazyConnection() as conn:
            with LazyChannel(conn) as ch:
                Queue(queue_name, ch).delete()

    def test_task_decorator(self):
        # Decorator without args
        self.assertTrue(isinstance(print_task, Task))
        # Decorator with args
        self.assertTrue(isinstance(print_task2, Task))

    def test_simple_task(self):
        self.clear_queue('kuyruk')
        print_task('hello world')

        result = run_kuyruk()
        assert 'hello world' in result.stdout

    def test_another_queue(self):
        self.clear_queue('another_queue')
        print_task2('hello another')

        result = run_kuyruk(queues='another_queue')
        assert 'hello another' in result.stdout
