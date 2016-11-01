import logging
import unittest

from mock import patch

from kuyruk import Kuyruk
from kuyruk import Task
from tests import tasks


logger = logging.getLogger(__name__)


class KuyrukTestCase(unittest.TestCase):

    def test_task_decorator(self):
        """Task decorator is applied successfully"""
        # Decorator without args
        self.assertTrue(isinstance(tasks.echo, Task))
        # Decorator with args
        self.assertTrue(isinstance(tasks.echo_another, Task))

    @patch('tests.tasks.must_be_called')
    def test_eager(self, mock_func):
        """Task.apply() is called successfully on eager mode"""
        tasks.eager_task()
        mock_func.assert_called_once_with()

    @patch('tests.tasks.must_be_called')
    def test_apply(self, mock_func):
        """Task.apply() is called successfully"""
        tasks.echo.apply("hello")
        mock_func.assert_called_once_with()

    def test_task_name(self):
        """Task.name is correct"""
        self.assertEqual(tasks.echo.name, 'tests.tasks:echo')

    def test_task_queue_name(self):
        """Task queue name is correct"""
        k = Kuyruk()

        def f():
            return

        t = k.task(f)
        self.assertEqual(t._queue_for_host(None), 'kuyruk')
        self.assertEqual(t._queue_for_host('foo'), 'kuyruk.foo')
