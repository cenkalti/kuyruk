import logging
import unittest

from kuyruk import Kuyruk, Task, Reject
from kuyruk.task import TaskResult
from util import run_kuyruk, clear

logger = logging.getLogger(__name__)


class KuyrukTestCase(unittest.TestCase):

    def test_task_decorator(self):
        """Does task decorator works correctly?"""
        # Decorator without args
        self.assertTrue(isinstance(print_task, Task))
        # Decorator with args
        self.assertTrue(isinstance(print_task2, Task))

    @clear('kuyruk')
    def test_simple_task(self):
        """Run a task on default queue"""
        print_task('hello world')
        result = run_kuyruk()
        assert 'hello world' in result.stdout

    @clear('another_queue')
    def test_another_queue(self):
        """Run a task on different queue"""
        print_task2('hello another')
        result = run_kuyruk(queues='another_queue')
        assert 'hello another' in result.stdout

    @clear('kuyruk')
    def test_exception(self):
        """Errored tasks must be retried every second"""
        raise_exception()
        result = run_kuyruk(seconds=2)
        self.assertEqual(result.stdout.count('ZeroDivisionError'), 2)

    @clear('kuyruk')
    def test_cold_shutdown(self):
        """If the worker is stuck on the task it can be stopped by
        invoking cold shutdown"""
        loop_forever()
        result = run_kuyruk(cold_shutdown=True, expect_error=True)
        assert 'Cold shutdown' in result.stderr

    def test_eager(self):
        """Test eager mode for using in test environments"""
        result = add(1, 2)
        assert isinstance(result, TaskResult)

    @clear('kuyruk')
    def test_reject(self):
        """Rejected tasks must be requeued again"""
        rejecting_task()
        result = run_kuyruk(seconds=2)
        self.assertEqual(result.stderr.count('Task is rejected'), 2)


kuyruk = Kuyruk()
# These functions below needs to be at module level in order that
# Kuyruk worker to determine their fully qualified name.


@kuyruk.task
def print_task(message):
    print message


@kuyruk.task(queue='another_queue')
def print_task2(message):
    print message


@kuyruk.task
def raise_exception():
    return 1 / 0


@kuyruk.task
def loop_forever():
    while 1:
        pass


@kuyruk.task(eager=True)
def add(a, b):
    return a + b


@kuyruk.task
def rejecting_task():
    raise Reject
