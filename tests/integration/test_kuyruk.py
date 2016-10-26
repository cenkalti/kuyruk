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
    RabbitMQ and Redis instances to pass succesfully.
    Just like the normal user they spawn a real worker process
    and make some assertion in their output.

    """
    def setUp(self):
        delete_queue('kuyruk')

    def test_simple_task(self):
        """Run a task on default queue"""
        tasks.echo('hello world')
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            worker.expect('hello world')
            worker.expect('Task is processed')

    def test_another_queue(self):
        """Run a task on another queue"""
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
        """Run signal handlers"""
        tasks.task_with_signal_handlers('hello world')
        presend_mock.assert_called_once()
        with run_kuyruk() as worker:
            worker.expect('function2')
            worker.expect('hello world')
            worker.expect('function5')

    def test_max_run_time(self):
        """Timeout long running task"""
        run_time = tasks.sleeping_task.max_run_time + 0.1
        tasks.sleeping_task(run_time)
        with run_kuyruk() as worker:
            worker.expect('Timeout')

    def test_worker_sigquit(self):
        """Ack current message and exit"""
        tasks.loop_forever()
        with run_kuyruk() as worker:
            worker.expect('looping forever')
            pid = get_pid('kuyruk: worker')
            os.kill(pid, signal.SIGUSR2)
            worker.expect('Dropping current task')
        assert len_queue("kuyruk") == 0, worker.get_output()

    def test_result_wait(self):
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            with tasks.add.run_in_queue(args=(1, 2)) as result:
                n = result.wait(2)
        assert n == 3

    def test_result_wait_timeout(self):
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            with tasks.just_sleep.run_in_queue(args=(10, )) as result:
                self.assertRaises(ResultTimeout, result.wait, 0.1)

    def test_result_wait_exception(self):
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            with self.assertRaises(RemoteException) as cm:
                with tasks.raise_exception.run_in_queue() as result:
                    result.wait(2)
            e = cm.exception
            assert e.type == 'exceptions.ZeroDivisionError'

    def test_result_wait_discard(self):
        with run_kuyruk() as worker:
            worker.expect('Consumer started')
            with self.assertRaises(RemoteException) as cm:
                with tasks.discard.run_in_queue() as result:
                    result.wait(2)
            e = cm.exception
            self.assertEqual(e.type, 'kuyruk.exceptions.Discard')
