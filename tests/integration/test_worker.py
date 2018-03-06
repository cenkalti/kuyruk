import json
import signal
import logging
import unittest

import amqp
from mock import patch

from kuyruk.exceptions import ResultTimeout, RemoteException
from tests import tasks
from tests.integration.util import run_worker, delete_queue, len_queue
from tests.integration.util import drop_connections, new_instance, wait_until


logger = logging.getLogger(__name__)


class WorkerTestCase(unittest.TestCase):
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
        with run_worker() as worker:
            worker.expect('Consumer started')
            worker.expect('hello world')
            worker.expect('Task is processed')

    def test_import_cache(self):
        """Task is run on default queue"""
        tasks.echo('hello world')
        tasks.echo('hello world')
        with run_worker() as worker:
            worker.expect('Consumer started')
            worker.expect('hello world')
            worker.expect('Task is processed')
            worker.expect('hello world')
            worker.expect('Task is processed')

    def test_another_queue(self):
        """Task is run on another queue"""
        delete_queue('another_queue')
        tasks.echo_another('hello another')
        assert len_queue('another_queue') == 1
        with run_worker(queue='another_queue') as worker:
            worker.expect('Consumer started')
            worker.expect('hello another')
            worker.expect('Task is processed')

    def test_exception(self):
        """Errored task message is discarded"""
        tasks.raise_exception()
        with run_worker() as worker:
            worker.expect('ZeroDivisionError')
        assert len_queue("kuyruk") == 0

    def test_declare_error(self):
        """Worker failed to start if existing queue arguments are different"""
        with tasks.kuyruk.channel() as ch:
            ch.queue_declare("kuyruk", durable=False)
        self.assertRaises(amqp.exceptions.PreconditionFailed,
                          tasks.echo,
                          args=('hello world', ))
        with run_worker(terminate=False) as worker:
            worker.expect('PRECONDITION_FAILED')

    def test_retry(self):
        """Errored task is retried"""
        tasks.retry_task()
        with run_worker() as worker:
            worker.expect('ZeroDivisionError')
        assert len_queue("kuyruk") == 0

    def test_reject(self):
        """Rejected task is requeued"""
        tasks.rejecting_task()
        with run_worker() as worker:
            worker.expect('Task is rejected')
            worker.expect('Task is rejected')
        assert len_queue("kuyruk") == 1

    def test_reject_delay(self):
        """Rejected task is requeued after delay"""
        tasks.rejecting_task_with_delay()
        with run_worker() as worker:
            worker.expect('Task is rejected')
            assert len_queue("kuyruk") == 0
            worker.expect('Task is rejected')
        assert len_queue("kuyruk") == 1

    @patch('tests.tasks.must_be_called')
    def test_before_after(self, presend_mock):
        """Signal handlers are run"""
        tasks.task_with_signal_handlers('hello world')
        presend_mock.assert_called_once()
        with run_worker() as worker:
            worker.expect('function2')
            worker.expect('hello world')
            worker.expect('function5')

    def test_task_max_run_time(self):
        """Worker raised Timeout if task runs too long"""
        run_time = tasks.sleeping_task.max_run_time + 0.1
        tasks.sleeping_task(run_time)
        with run_worker() as worker:
            worker.expect('Timeout')

    def test_worker_sigquit(self):
        """Worker drops the task on SIGQUIT"""
        tasks.loop_forever()
        with run_worker() as worker:
            worker.expect('looping forever')
            worker.send_signal(signal.SIGUSR2)
            worker.expect('Dropping current task')
        assert len_queue("kuyruk") == 0, worker.get_output()

    def test_result_wait(self):
        """Result is received"""
        with run_worker() as worker:
            worker.expect('Consumer started')
            n = tasks.add.send_to_queue(args=(1, 2), wait_result=2)
        assert n == 3

    def test_result_serialization_error(self):
        """Worker does not crash for unserializable result"""
        with run_worker() as worker:
            with self.assertRaises(RemoteException) as cm:
                tasks.object_result.send_to_queue(wait_result=2)
            worker.expect('Cannot serialize result as JSON')
        e = cm.exception
        assert e.type.endswith('.TypeError')

    def test_result_wait_timeout(self):
        """ResultTimeout is raised if result is not received"""
        with run_worker() as worker:
            worker.expect('Consumer started')
            with self.assertRaises(ResultTimeout):
                tasks.just_sleep.send_to_queue(args=(1, ), wait_result=0.1)

    def test_result_wait_exception(self):
        """RemoteException is raised on worker exceptions"""
        with run_worker() as worker:
            worker.expect('Consumer started')
            with self.assertRaises(RemoteException) as cm:
                tasks.raise_exception.send_to_queue(wait_result=2)
        e = cm.exception
        # exceptions.ZeroDivisionError in Python 2
        # builtins.ZeroDivisionError in Python 3
        assert e.type.endswith('.ZeroDivisionError')

    def test_result_wait_discard(self):
        """RemoteException is raised on discarded tasks"""
        with run_worker() as worker:
            worker.expect('Consumer started')
            with self.assertRaises(RemoteException) as cm:
                tasks.discard.send_to_queue(wait_result=2)
            e = cm.exception
            self.assertEqual(e.type, 'kuyruk.exceptions.Discard')

    def test_worker_max_run_time(self):
        """Worker shutdowns gracefully after WORKER_MAX_RUN_TIME seconds"""
        with run_worker(max_run_time=1) as worker:
            worker.expect('Consumer started')
            worker.expect('Shutdown requested', timeout=2)

    def test_heartbeat_error(self):
        """HeartbeatError is raised on disconnect"""
        tasks.just_sleep(10)
        with run_worker(terminate=False) as worker:
            worker.expect('sleeping 10 seconds')
            wait_until(lambda: drop_connections() > 0, 5)
            worker.expect('HeartbeatError')
            worker.expect_exit(1)

    def test_import_app_error(self):
        """TypeError is raised when app is not istance of Kuyruk"""
        with run_worker(terminate=False, app='sys.argv') as worker:
            worker.expect('TypeError')
            worker.expect_exit(1)

    def test_invalid_json(self):
        """Message is dropped when JSON is not valid"""
        with run_worker() as worker:
            worker.expect('Consumer started')
            with new_instance().channel() as ch:
                msg = amqp.Message(body='foo')
                ch.basic_publish(msg, exchange='', routing_key='kuyruk')
            worker.expect('Cannot decode message')
        self.assertEqual(len_queue('kuyruk'), 0)

    def test_invalid_task_path(self):
        """Message is dropped when task cannot be imported"""
        with run_worker() as worker:
            worker.expect('Consumer started')
            with new_instance().channel() as ch:
                desc = {'module': 'kuyruk', 'function': 'foo'}
                body = json.dumps(desc)
                msg = amqp.Message(body=body)
                ch.basic_publish(msg, exchange='', routing_key='kuyruk')
            worker.expect('Cannot import task')
        self.assertEqual(len_queue('kuyruk'), 0)

    def test_sigint(self):
        """Worker shuts down gracefully on SIGINT"""
        tasks.just_sleep(1)
        tasks.just_sleep(1)
        with run_worker(terminate=False) as worker:
            worker.expect('Processing task')
            worker.send_signal(signal.SIGINT)
            worker.expect('Task is successful')
            worker.expect_exit(0)
        assert len_queue("kuyruk") == 1, worker.get_output()

    def test_sigusr1(self):
        """Print stacktrace on SIGUSR1"""
        with run_worker() as worker:
            worker.expect('Consumer started')
            worker.send_signal(signal.SIGUSR1)
            worker.expect('traceback.format_stack')

    def test_batch(self):
        """Batch tasks are run"""
        li = [tasks.echo.subtask(args=("foo %i" % i, )) for i in range(2)]
        tasks.kuyruk.send_tasks_to_queue(li)
        with run_worker() as worker:
            worker.expect('Consumer started')
            worker.expect('foo 0')
            worker.expect('foo 1')
            worker.expect('Task is processed')
