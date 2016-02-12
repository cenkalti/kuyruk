import logging
import unittest

from mock import patch

from kuyruk import Task
from tests import tasks


logger = logging.getLogger(__name__)


class KuyrukTestCase(unittest.TestCase):

    def test_task_decorator(self):
        """Does task decorator works correctly?"""
        # Decorator without args
        self.assertTrue(isinstance(tasks.echo, Task))
        # Decorator with args
        self.assertTrue(isinstance(tasks.echo_another, Task))

    @patch('tests.tasks.must_be_called')
    def test_eager(self, mock_func):
        """Test eager mode for using in test environments"""
        tasks.eager_task()
        mock_func.assert_called_once_with()

    @patch('tests.tasks.must_be_called')
    def test_apply(self, mock_func):
        """Test Task.apply()"""
        tasks.echo.apply("hello")
        mock_func.assert_called_once_with()

    def test_task_name(self):
        self.assertEqual(tasks.echo.name, 'tests.tasks:echo')
