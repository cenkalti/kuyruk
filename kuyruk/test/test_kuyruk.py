import signal
import inspect
import logging
import unittest

import tasks
from kuyruk import Task
from kuyruk.task import TaskResult
from util import *

logger = logging.getLogger(__name__)

logger.debug('Process id: %s', os.getpid())
logger.debug('Process group id: %s', os.getpgrp())


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
        with run_kuyruk() as master:
            master.expect('hello world')

    def test_another_queue(self):
        """Run a task on different queue"""
        tasks.print_task2('hello another')
        with run_kuyruk(queues='another_queue') as master:
            master.expect('another_queue')
            master.expect('hello another')

    def test_exception(self):
        """Errored task message is discarded"""
        tasks.raise_exception()
        with run_kuyruk() as master:
            master.expect('ZeroDivisionError')
        assert is_empty('kuyruk')

    def test_retry(self):
        """Errored tasks must be retried"""
        tasks.retry_task()
        with run_kuyruk() as master:
            master.expect('ZeroDivisionError')
            master.expect('ZeroDivisionError')
        assert is_empty('kuyruk')

    def test_cold_shutdown(self):
        """If the worker is stuck on the task it can be stopped by
        invoking cold shutdown"""
        tasks.loop_forever()
        with run_kuyruk(terminate=False) as master:
            master.expect('looping forever')
            master.send_signal(signal.SIGINT)
            master.expect('Warm shutdown')
            master.expect('Handled SIGINT')
            master.send_signal(signal.SIGINT)
            master.expect('Cold shutdown')
            master.expect_exit(0)
            wait_until(not_running, timeout=TIMEOUT)

    def test_eager(self):
        """Test eager mode for using in test environments"""
        result = tasks.eager_task()
        assert isinstance(result, TaskResult)
        self.assertTrue(tasks.eager_called)

    def test_reject(self):
        """Rejected tasks must be requeued again"""
        tasks.rejecting_task()
        with run_kuyruk() as master:
            master.expect('Task is rejected')
            master.expect('Task is rejected')
        assert not is_empty('kuyruk')

    def test_respawn(self):
        """Respawn a new worker if dead

        This test also covers the broker disconnect case because when the
        connection drops the master worker will raise an unhandled exception.
        This exception will cause the worker to exit. After exiting, master
        worker will spawn a new master worker.

        """
        _pid = lambda: get_pid('kuyruk: worker')
        with run_kuyruk() as master:
            master.expect('Start consuming')
            pid1 = _pid()
            os.kill(pid1, signal.SIGKILL)
            master.expect('Spawning new worker')
            master.expect('Start consuming')
            pid2 = _pid()
        assert pid2 > pid1

    def test_save_failed(self):
        """Failed tasks are saved to another queue"""
        delete_queue('kuyruk_failed')
        tasks.raise_exception()
        with run_kuyruk(save_failed_tasks=True) as master:
            master.expect('ZeroDivisionError')
            master.expect('No retry left')
            master.expect('Saving failed task')
            master.expect('Saved')
        assert is_empty('kuyruk')
        assert not is_empty('kuyruk_failed')

        run_requeue()
        assert is_empty('kuyruk_failed')
        assert not is_empty('kuyruk')

    def test_dead_master(self):
        """If master is dead worker should exit gracefully"""
        tasks.print_task('hello world')
        with run_kuyruk(terminate=False) as master:
            master.expect('hello world')
            master.kill()
            master.expect_exit(-signal.SIGKILL)
            wait_until(not_running, timeout=TIMEOUT)

    def test_before_after(self):
        """Before and after task functions are run"""
        tasks.task_with_functions('hello world')
        with run_kuyruk() as master:
            master.expect('function1')
            master.expect('function2')
            master.expect('hello world')
            master.expect('function3')
            master.expect('function4')
            master.expect('function5')

    def test_extend(self):
        """Extend task class"""
        tasks.use_session()
        with run_kuyruk() as master:
            master.expect('Opening session')
            master.expect('Closing session')

    def test_class_task(self):
        cat = tasks.Cat(1, 'Felix')
        self.assertTrue(isinstance(tasks.Cat.meow, Task))
        self.assertTrue(inspect.ismethod(cat.meow))

        cat.meow('hello')
        with run_kuyruk() as master:
            master.expect('Felix says: hello')

    def test_task_name(self):
        self.assertEqual(tasks.Cat.meow.name, 'kuyruk.test.tasks:Cat.meow')
        self.assertEqual(tasks.print_task.name, 'kuyruk.test.tasks:print_task')
