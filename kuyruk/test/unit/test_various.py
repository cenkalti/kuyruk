import logging
import unittest

from mock import patch

from kuyruk import Task
from kuyruk.task import TaskResult
from kuyruk.test import tasks


logger = logging.getLogger(__name__)


class KuyrukTestCase(unittest.TestCase):

    def test_task_decorator(self):
        """Does task decorator works correctly?"""
        # Decorator without args
        self.assertTrue(isinstance(tasks.print_task, Task))
        # Decorator with args
        self.assertTrue(isinstance(tasks.print_task2, Task))

    @patch('kuyruk.test.tasks.must_be_called')
    def test_eager(self, mock_func):
        """Test eager mode for using in test environments"""
        result = tasks.eager_task()
        assert isinstance(result, TaskResult)
        mock_func.assert_called_once_with()

    @patch('kuyruk.test.tasks.must_be_called')
    def test_apply(self, mock_func):
        """Test Task.apply()"""
        result = tasks.print_task.apply("hello")
        assert isinstance(result, TaskResult)
        mock_func.assert_called_once_with()

    @patch('kuyruk.test.tasks.must_be_called')
    def test_class_task_eager(self, mock_func):
        cat = tasks.Cat(1, 'Felix')
        cat.meow_eager('Oh my god')
        mock_func.assert_called_once_with()

    @patch('kuyruk.test.tasks.must_be_called')
    def test_class_task_apply(self, mock_func):
        cat = tasks.Cat(1, 'Felix')
        cat.meow.apply('Oh my god')
        mock_func.assert_called_once_with()

    @patch('kuyruk.test.tasks.must_be_called')
    def test_arg_class_eager(self, mock_func):
        cat = tasks.Cat(1, 'Felix')
        tasks.jump_eager(cat)
        mock_func.assert_called_once_with('Felix')

    @patch('kuyruk.test.tasks.must_be_called')
    def test_arg_class_apply(self, mock_func):
        cat = tasks.Cat(1, 'Felix')
        tasks.jump.apply(cat)
        mock_func.assert_called_once_with('Felix')

    def test_task_name(self):
        self.assertEqual(tasks.Cat.meow.name, 'kuyruk.test.tasks:Cat.meow')
        self.assertEqual(tasks.print_task.name, 'kuyruk.test.tasks:print_task')
