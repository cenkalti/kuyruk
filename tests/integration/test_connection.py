import threading
import unittest
from unittest.mock import patch

from kuyruk import Kuyruk
from tests import tasks
from tests.integration.util import delete_queue, drop_connections, run_worker


class ConnectionTestCase(unittest.TestCase):
    def setUp(self):
        delete_queue('kuyruk')
        tasks.kuyruk._remove_connection()

    @patch.object(Kuyruk, '_new_connection', wraps=tasks.kuyruk._new_connection)
    def test_serial_tasks(self, new_connection):
        tasks.echo('foo')
        tasks.echo('bar')

        with run_worker() as worker:
            worker.expect('foo')
            worker.expect('Task is successful')
            worker.expect('bar')
            worker.expect('Task is successful')

        assert new_connection.call_count == 1

    @patch.object(Kuyruk, '_new_connection', wraps=tasks.kuyruk._new_connection)
    def test_task_in_new_thread(self, new_connection):
        tasks.echo('in_main')

        thread = threading.Thread(target=tasks.echo, args=('in_thread',), daemon=True)
        thread.start()
        thread.join()

        with run_worker() as worker:
            worker.expect('in_main')
            worker.expect('Task is successful')
            worker.expect('in_thread')
            worker.expect('Task is successful')

        assert new_connection.call_count == 1
