import logging
import unittest

from kuyruk import Kuyruk, Task
from util import run_kuyruk, clear

logger = logging.getLogger(__name__)


class KuyrukTestCase(unittest.TestCase):

    def test_task_decorator(self):
        # Decorator without args
        self.assertTrue(isinstance(print_task, Task))
        # Decorator with args
        self.assertTrue(isinstance(print_task2, Task))

    @clear('kuyruk')
    def test_simple_task(self):
        print_task('hello world')
        result = run_kuyruk()
        assert 'hello world' in result.stdout

    @clear('another_queue')
    def test_another_queue(self):
        print_task2('hello another')
        result = run_kuyruk(queues='another_queue')
        assert 'hello another' in result.stdout


# These 2 functions below needs to be at module level in order that Kuyruk
# to determine their fully qualified name.
kuyruk = Kuyruk()


@kuyruk.task
def print_task(message):
    print message


@kuyruk.task(queue='another_queue')
def print_task2(message):
    print message
