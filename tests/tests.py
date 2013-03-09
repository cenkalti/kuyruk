import os
import sys
import unittest
import threading
from time import sleep
from contextlib import contextmanager

# HACK: Prepend ../ to PYTHONPATH so that we can import muhtar form there.
TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.realpath(os.path.join(TESTS_ROOT, '..')))

from kuyruk import Kuyruk
from kuyruk.task import Task


@contextmanager
def run_kuyruk(kuyruk, queue='kuyruk'):
    thread = threading.Thread(target=kuyruk.run, args=(queue, ))
    thread.start()
    yield
    sleep(1)
    kuyruk.exit = True
    thread.join()

kuyruk = Kuyruk()
called = False
@kuyruk.task
def print_task(message):
    global called
    called = True
    print message


class KuyrukTestCase(unittest.TestCase):

    def test_task_decorator(self):
        self.assertIsInstance(print_task, Task)

        @kuyruk.task(queue='another_queue')
        def _task():
            pass

        self.assertIsInstance(_task, Task)

    def test_simple_task(self):
        with run_kuyruk(kuyruk):
            print_task('hello world')

        self.assertEqual(called, True)


if __name__ == '__main__':
    unittest.main()
