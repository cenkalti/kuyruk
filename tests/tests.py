import os
import sys
import unittest
import threading

# HACK: Prepend ../ to PYTHONPATH so that we can import muhtar form there.
TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.realpath(os.path.join(TESTS_ROOT, '..')))

from kuyruk import Kuyruk
from kuyruk.task import Task


def run_kuyruk(kuyruk, queue='kuyruk'):
    thread = threading.Thread(target=kuyruk.run, args=(queue, ))
    thread.start()


def print_task(message):
    print message


class KuyrukTestCase(unittest.TestCase):

    def test_task_decorator(self):
        kuyruk = Kuyruk()

        @kuyruk.task
        def print_task(message):
            print message

        self.assertIsInstance(print_task, Task)

    def test_simple_task(self):
        kuyruk = Kuyruk()

        _print = kuyruk.task(print_task)

        run_kuyruk(kuyruk)

        _print('hello world')

        import time
        time.sleep(2)
        kuyruk.exit = True


if __name__ == '__main__':
    unittest.main()
