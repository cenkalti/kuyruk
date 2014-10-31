import os
import signal
import logging
import unittest

import rabbitpy
import rabbitpy.exceptions
from mock import patch

from kuyruk import Kuyruk, Task
import kuyruk.task
from kuyruk.task import BoundTask
from kuyruk.test import tasks
from kuyruk.test.integration.util import run_kuyruk, wait_until, \
    not_running, get_pid, TIMEOUT

logger = logging.getLogger(__name__)

logger.debug('Process id: %s', os.getpid())
logger.debug('Process group id: %s', os.getpgrp())

kuyruk.task._DECLARE_ALWAYS = True


class KuyrukTestCase(unittest.TestCase):
    """
    Tests here are mostly integration tests. They require a running
    RabbitMQ and Redis instances to pass succesfully.
    Just like the normal user they spawn a real worker process
    and make some assertion in their output.

    """
    def setUp(self):
        self.kuyruk = Kuyruk()
        self._channel = self.kuyruk.channel()
        self.queue = rabbitpy.Queue(self._channel, "kuyruk", durable=True)
        try:
            self.queue.delete()
        except rabbitpy.exceptions.AMQPNotFound:
            pass

    def tearDown(self):
        self._channel.close()

    def test_simple_task(self):
        """Run a task on default queue"""
        tasks.print_task('hello world')
        with run_kuyruk() as worker:
            worker.expect('Start consuming')
            worker.expect('hello world')
            worker.expect('Task is processed')

    def test_another_queue(self):
        """Run a task on different queue"""
        tasks.print_task2('hello another')
        with run_kuyruk(queue='another_queue') as worker:
            worker.expect('Start consuming')
            worker.expect('another_queue')
            worker.expect('hello another')
            worker.expect('Task is processed')

    def test_exception(self):
        """Errored task message is discarded"""
        tasks.raise_exception()
        with run_kuyruk() as worker:
            worker.expect('ZeroDivisionError')
        assert len(self.queue) == 0

    def test_retry(self):
        """Errored tasks must be retried"""
        tasks.retry_task()
        with run_kuyruk() as worker:
            worker.expect('ZeroDivisionError')
            worker.expect('ZeroDivisionError')
        assert len(self.queue) == 0

    def test_cold_shutdown(self):
        """If the worker is stuck on the task it can be stopped by
        invoking cold shutdown"""
        tasks.loop_forever()
        with run_kuyruk(terminate=False) as worker:
            worker.expect('looping forever')
            worker.send_signal(signal.SIGINT)
            worker.expect('Warm shutdown')
            worker.expect('Handled SIGINT')
            worker.send_signal(signal.SIGINT)
            worker.expect('Cold shutdown')
            worker.expect_exit(0)
            wait_until(not_running, timeout=TIMEOUT)

    def test_reject(self):
        """Rejected tasks must be requeued again"""
        tasks.rejecting_task()
        with run_kuyruk() as worker:
            worker.expect('Task is rejected')
            worker.expect('Task is rejected')
        assert len(self.queue) == 1

    @patch('kuyruk.test.tasks.must_be_called')
    def test_before_after(self, mock_func):
        """Before and after task functions are run"""
        tasks.task_with_functions('hello world')
        mock_func.assert_called_once_with()
        with run_kuyruk() as worker:
            worker.expect('function1')
            worker.expect('function2')
            worker.expect('hello world')
            worker.expect('function3')
            worker.expect('function4')
            worker.expect('function5')

    def test_extend(self):
        """Extend task class"""
        tasks.use_session()
        with run_kuyruk() as worker:
            worker.expect('Opening session')
            worker.expect('Closing session')

    def test_class_task(self):
        cat = tasks.Cat(1, 'Felix')
        self.assertTrue(isinstance(tasks.Cat.meow, Task))
        self.assertTrue(isinstance(cat.meow, BoundTask))

        cat.meow('Oh my god')
        with run_kuyruk() as worker:
            worker.expect('Oh my god')

    def test_arg_class(self):
        cat = tasks.Cat(1, 'Felix')
        tasks.jump(cat)
        with run_kuyruk() as worker:
            worker.expect('Felix jumps high!')
            worker.expect('Called with Felix')

    def test_max_run_time(self):
        """Timeout long running task"""
        run_time = tasks.sleeping_task.max_run_time + 1
        tasks.sleeping_task(run_time)
        with run_kuyruk() as worker:
            worker.expect('raise Timeout')

    def test_worker_sigquit(self):
        """Ack current message and exit"""
        tasks.loop_forever()
        with run_kuyruk(terminate=False) as worker:
            worker.expect('looping forever')
            pid = get_pid('kuyruk: worker')
            os.kill(pid, signal.SIGQUIT)
            worker.expect('Acking current task')
            worker.expect('Exiting')
            worker.expect_exit(0)
        assert len(self.queue) == 0, worker.get_output()
