import signal
import logging
import unittest
import threading
from time import sleep
from Queue import deque

from kuyruk import Kuyruk, Task, Reject, Queue
from kuyruk.connection import LazyConnection
from kuyruk.task import TaskResult
from util import run_kuyruk, kill_kuyruk, clear, delete_queue, get_pids

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
        """Errored task message is discarded"""
        raise_exception()
        result = run_kuyruk()
        assert 'ZeroDivisionError' in result.stderr
        self.assert_empty('kuyruk')

    @clear('kuyruk')
    def test_retry(self):
        """Errored tasks must be retried"""
        retry_task()
        result = run_kuyruk(seconds=2)
        self.assertEqual(result.stderr.count('ZeroDivisionError'), 2)
        self.assert_empty('kuyruk')

    @clear('kuyruk')
    def test_cold_shutdown(self):
        """If the worker is stuck on the task it can be stopped by
        invoking cold shutdown"""
        loop_forever()
        result = run_kuyruk(cold_shutdown=True, expect_error=True)
        assert 'Cold shutdown' in result.stderr

    def test_eager(self):
        """Test eager mode for using in test environments"""
        result = eager_task()
        assert isinstance(result, TaskResult)
        self.assertTrue(eager_called)

    @clear('kuyruk')
    def test_reject(self):
        """Rejected tasks must be requeued again"""
        rejecting_task()
        result = run_kuyruk(seconds=2)
        self.assertEqual(result.stderr.count('Task is rejected'), 2)

    @clear('kuyruk')
    def test_delete_queue(self):
        """Delete queue while worker is running"""
        def delete_after(seconds):
            sleep(seconds)
            delete_queue('kuyruk')
        t = threading.Thread(target=delete_after, args=(1, ))
        t.start()
        result = run_kuyruk(seconds=2)
        self.assertEqual(result.stderr.count('Declaring queue'), 2)

    def test_respawn(self):
        """Respawn a new worker if dead

        This test also covers the broker disconnect case because when the
        connection drops the child worker will raise an unhandled exception.
        This exception will cause the worker to exit. After exiting, master
        worker will spawn a new child worker.

        """
        def kill_worker():
            sleep(1)
            pid_worker = get_pids('kuyruk: worker')[0]
            pids.append(pid_worker)
            kill_kuyruk(signal.SIGKILL, worker='worker')

        def get_worker_pid():
            sleep(2)
            pid_worker = get_pids('kuyruk: worker')[0]
            pids.append(pid_worker)

        pids = deque()
        threading.Thread(target=kill_worker).start()
        threading.Thread(target=get_worker_pid).start()
        result = run_kuyruk(seconds=3)
        assert 'Spawning new worker' in result.stderr
        assert pids[1] > pids[0]

    def assert_empty(self, queue):
        queue = Queue(queue, LazyConnection().channel())
        self.assertEqual(len(queue), 0)


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


@kuyruk.task(retry=1)
def retry_task():
    return 1 / 0


@kuyruk.task
def loop_forever():
    while 1:
        pass


@kuyruk.task(eager=True)
def eager_task():
    eager_called.append(1)

eager_called = []


@kuyruk.task
def rejecting_task():
    raise Reject
