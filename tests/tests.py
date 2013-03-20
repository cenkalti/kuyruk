#!/usr/bin/env python
import os
import sys
import logging
import unittest
import threading
import traceback
from time import sleep
from contextlib import contextmanager

# HACK: Prepend ../ to PYTHONPATH so that we can import muhtar form there.
TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.realpath(os.path.join(TESTS_ROOT, '..')))

from kuyruk import Kuyruk
from kuyruk.task import Task


def exit_on_excption(f):
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            traceback.print_exc()
    return inner


@contextmanager
def run_kuyruk(kuyruk, queue='kuyruk'):
    target = exit_on_excption(kuyruk.run)
    thread = threading.Thread(target=target, args=(queue, ))
    thread.start()
    yield
    sleep(2)
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
    logging.getLogger('pika').setLevel(logging.WARNING)
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
