import os
import signal
import logging
import unittest

from mock import patch

from kuyruk.exceptions import ResultTimeout, RemoteException
from tests import tasks
from tests.integration.util import run_kuyruk, get_pid, delete_queue, len_queue


logger = logging.getLogger(__name__)


class KuyrukTestCase(unittest.TestCase):
    """
    Tests here are mostly integration tests. They require a running
    RabbitMQ and Redis instances to pass successfully.
    Just like the normal user they spawn a real worker process
    and make some assertion in their output.

    """
    def setUp(self):
        delete_queue('kuyruk')

    def test_simple_task(self):
        """Task is run on default queue"""
        tasks.echo('hello world')
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            worker.expect('hello world')
            worker.expect('Task is processed')

    def test_another_queue(self):
        """Task is run on another queue"""
        tasks.echo_another('hello another')
        with run_kuyruk(queue='another_queue') as worker:
            worker.expect('Consumer started')
            worker.expect('another_queue')
            worker.expect('hello another')
            worker.expect('Task is processed')

    def test_exception(self):
        """Errored task message is discarded"""
        tasks.raise_exception()
        with run_kuyruk() as worker:
            worker.expect('ZeroDivisionError')
        assert len_queue("kuyruk") == 0

    def test_retry(self):
        """Errored task is retried"""
        tasks.retry_task()
        with run_kuyruk() as worker:
            worker.expect('ZeroDivisionError')
            worker.expect('ZeroDivisionError')
        assert len_queue("kuyruk") == 0

    def test_reject(self):
        """Rejected task is requeued"""
        tasks.rejecting_task()
        with run_kuyruk() as worker:
            worker.expect('Task is rejected')
            worker.expect('Task is rejected')
        assert len_queue("kuyruk") == 1

    @patch('tests.tasks.must_be_called')
    def test_before_after(self, presend_mock):
        """Signal handlers are run"""
        tasks.task_with_signal_handlers('hello world')
        presend_mock.assert_called_once()
        with run_kuyruk() as worker:
            worker.expect('function2')
            worker.expect('hello world')
            worker.expect('function5')

    def test_max_run_time(self):
        """Worker raised Timeout if task runs too long"""
        run_time = tasks.sleeping_task.max_run_time + 0.1
        tasks.sleeping_task(run_time)
        with run_kuyruk() as worker:
            worker.expect('Timeout')

    def test_worker_sigquit(self):
        """Worker drops the task on SIGQUIT"""
        tasks.loop_forever()
        with run_kuyruk() as worker:
            worker.expect('looping forever')
            pid = get_pid('kuyruk: worker')
            os.kill(pid, signal.SIGUSR2)
            worker.expect('Dropping current task')
        assert len_queue("kuyruk") == 0, worker.get_output()

    def test_result_wait(self):
        """Result is received"""
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            n = tasks.add.send_to_queue(args=(1, 2), wait_result=2)
        assert n == 3

    def test_result_wait_timeout(self):
        """ResultTimeout is raised if result is not received"""
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            self.assertRaises(ResultTimeout, tasks.just_sleep.send_to_queue,
                              args=(10, ), wait_result=0.1)

    def test_result_wait_exception(self):
        """RemoteException is raised on worker exceptions"""
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            with self.assertRaises(RemoteException) as cm:
                tasks.raise_exception.send_to_queue(wait_result=2)
            e = cm.exception
            # exceptions.ZeroDivisionError in Python 2
            # builtins.ZeroDivisionError in Python 3
            assert e.type.endswith('.ZeroDivisionError')

    def test_result_wait_discard(self):
        """RemoteException is raised on discarded tasks"""
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            with self.assertRaises(RemoteException) as cm:
                tasks.discard.send_to_queue(wait_result=2)
            e = cm.exception
            self.assertEqual(e.type, 'kuyruk.exceptions.Discard')
