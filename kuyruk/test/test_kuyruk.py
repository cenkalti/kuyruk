import signal
import inspect
import logging
import unittest

import tasks
from kuyruk import Task
from kuyruk.task import TaskResult
from util import *

logger = logging.getLogger(__name__)


class KuyrukTestCase(unittest.TestCase):

    def setUp(self):
        delete_queue('kuyruk')

    def test_task_decorator(self):
        """Does task decorator works correctly?"""
        # Decorator without args
        self.assertTrue(isinstance(tasks.print_task, Task))
        # Decorator with args
        self.assertTrue(isinstance(tasks.print_task2, Task))

    def test_simple_task(self):
        """Run a task on default queue"""
        tasks.print_task('hello world')
        with run_kuyruk() as child:
            child.expect('hello world')

    def test_another_queue(self):
        """Run a task on different queue"""
        tasks.print_task2('hello another')
        with run_kuyruk(queues='another_queue') as child:
            child.expect('hello another')

    def test_exception(self):
        """Errored task message is discarded"""
        tasks.raise_exception()
        with run_kuyruk() as child:
            child.expect('ZeroDivisionError')
        assert is_empty('kuyruk')

    def test_retry(self):
        """Errored tasks must be retried"""
        tasks.retry_task()
        with run_kuyruk() as child:
            child.expect('ZeroDivisionError')
            child.expect('ZeroDivisionError')
        assert is_empty('kuyruk')

    def test_cold_shutdown(self):
        """If the worker is stuck on the task it can be stopped by
        invoking cold shutdown"""
        tasks.loop_forever()
        with run_kuyruk(terminate=False) as child:
            child.expect('looping forever')
            child.send_signal(signal.SIGINT)
            child.expect('Warm shutdown')
            child.expect('Handled SIGINT')
            child.send_signal(signal.SIGINT)
            child.expect('Cold shutdown')
            sleep_until(not_running, timeout=TIMEOUT)

    def test_eager(self):
        """Test eager mode for using in test environments"""
        result = tasks.eager_task()
        assert isinstance(result, TaskResult)
        self.assertTrue(tasks.eager_called)

    def test_reject(self):
        """Rejected tasks must be requeued again"""
        tasks.rejecting_task()
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
            os.kill(pid1, signal.SIGKILL)
            child.expect('Spawning new worker')
            child.expect('Starting consume')
            pid2 = _pid()
        assert pid2 > pid1

    def test_save_failed(self):
        """Failed tasks are saved to another queue"""
        delete_queue('kuyruk_failed')
        tasks.raise_exception()
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

    def test_dead_master(self):
        """If master is dead worker should exit gracefully"""
        tasks.print_task('hello world')
        with run_kuyruk(terminate=False) as child:
            child.expect('hello world')
            child.kill()
            sleep_until(not_running, timeout=TIMEOUT)

    def test_before_after(self):
        """Before and after task functions are run"""
        tasks.task_with_functions('hello world')
        with run_kuyruk() as child:
            child.expect('function1')
            child.expect('function2')
            child.expect('hello world')
            child.expect('function3')
            child.expect('function4')

    def test_class_task(self):
        cat = tasks.Cat(1, 'Felix')
        self.assertTrue(isinstance(tasks.Cat.meow, Task))
        self.assertTrue(inspect.ismethod(cat.meow))

        cat.meow('hello')
        with run_kuyruk() as child:
            child.expect('Felix says: hello')

    def test_task_name(self):
        self.assertEqual(tasks.Cat.meow.name, 'kuyruk.test.tasks:Cat.meow')
        self.assertEqual(tasks.print_task.name, 'kuyruk.test.tasks:print_task')
