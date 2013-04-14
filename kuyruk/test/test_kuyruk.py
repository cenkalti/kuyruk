import logging
import unittest
from time import sleep

from kuyruk import Kuyruk, Task, Reject
from kuyruk.task import TaskResult
from util import run_kuyruk, kill_worker, delete_queue, get_pid, run_requeue
from util import is_empty, sleep_until, not_running, TIMEOUT

logger = logging.getLogger(__name__)


class KuyrukTestCase(unittest.TestCase):

    def setUp(self):
        delete_queue('kuyruk')

    def test_task_decorator(self):
        """Does task decorator works correctly?"""
        # Decorator without args
        self.assertTrue(isinstance(print_task, Task))
        # Decorator with args
        self.assertTrue(isinstance(print_task2, Task))

    def test_simple_task(self):
        """Run a task on default queue"""
        print_task('hello world')
        with run_kuyruk() as child:
            child.expect('hello world')

    def test_another_queue(self):
        """Run a task on different queue"""
        print_task2('hello another')
        with run_kuyruk(queues='another_queue') as child:
            child.expect('hello another')

    def test_exception(self):
        """Errored task message is discarded"""
        raise_exception()
        with run_kuyruk() as child:
            child.expect('ZeroDivisionError')
        assert is_empty('kuyruk')

    def test_retry(self):
        """Errored tasks must be retried"""
        retry_task()
        with run_kuyruk() as child:
            child.expect('ZeroDivisionError')
            child.expect('ZeroDivisionError')
        assert is_empty('kuyruk')

    def test_cold_shutdown(self):
        """If the worker is stuck on the task it can be stopped by
        invoking cold shutdown"""
        loop_forever()
        with run_kuyruk(terminate=False) as child:
            child.expect('looping forever')
            child.sendintr()
            child.expect('Warm shutdown')
            child.sendintr()
            child.expect('Cold shutdown')
            sleep_until(not_running, timeout=TIMEOUT)

    def test_eager(self):
        """Test eager mode for using in test environments"""
        result = eager_task()
        assert isinstance(result, TaskResult)
        self.assertTrue(eager_called)

    def test_reject(self):
        """Rejected tasks must be requeued again"""
        rejecting_task()
        with run_kuyruk() as child:
            child.expect('Task is rejected')
            child.expect('Task is rejected')
        assert not is_empty('kuyruk')

    def test_respawn(self):
        """Respawn a new worker if dead

        This test also covers the broker disconnect case because when the
        connection drops the child worker will raise an unhandled exception.
        This exception will cause the worker to exit. After exiting, master
        worker will spawn a new child worker.

        """
        _pid = lambda: get_pid('kuyruk: worker')
        with run_kuyruk() as child:
            child.expect('Starting consume')
            pid1 = _pid()
            kill_worker()
            child.expect('Spawning new worker')
            child.expect('Starting consume')
            pid2 = _pid()
        assert pid2 > pid1

    def test_save_failed(self):
        """Failed tasks are saved to another queue"""
        delete_queue('kuyruk_failed')
        raise_exception()
        with run_kuyruk(save_failed_tasks=True) as child:
            child.expect('ZeroDivisionError')
            child.expect('No retry left')
            child.expect('Saving failed task')
            child.expect('Saved')
        assert is_empty('kuyruk')
        assert not is_empty('kuyruk_failed')

        run_requeue()
        assert is_empty('kuyruk_failed')
        assert not is_empty('kuyruk')


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
        print 'looping forever'


@kuyruk.task(eager=True)
def eager_task():
    eager_called.append(1)

eager_called = []


@kuyruk.task
def rejecting_task():
    raise Reject


@kuyruk.task
def sleeping_task(seconds):
    sleep(seconds)
