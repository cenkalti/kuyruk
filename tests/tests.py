#!/usr/bin/env python
import os
import sys
import logging
import unittest
import threading
import traceback
from time import sleep
from contextlib import contextmanager

# HACK: Prepend ../ to PYTHONPATH so that we can import kuyruk form there.
TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.realpath(os.path.join(TESTS_ROOT, '..')))

from kuyruk import Kuyruk, Task

kuyruk = Kuyruk()
called = False


@kuyruk.task
def print_task(message):
    global called
    called = True
    print message


@kuyruk.task(queue='another_queue')
def print_task2(message):
    global called
    called = True
    print message


class KuyrukTestCase(unittest.TestCase):

    def setUp(self):
        global called
        called = False

    def test_task_decorator(self):
        self.assertIsInstance(print_task, Task)
        self.assertIsInstance(print_task2, Task)

    def test_simple_task(self):
        with run_kuyruk(kuyruk):
            print_task('hello world')

        self.assertEqual(called, True)


@contextmanager
def run_kuyruk(kuyruk):
    target = exit_on_excption(kuyruk.run)
    thread = threading.Thread(target=target)
    thread.start()
    yield
    sleep(1)
    kuyruk.exit = True
    thread.join()


def exit_on_excption(f):
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            traceback.print_exc()
    return inner


if __name__ == '__main__':
    logging.getLogger('pika').setLevel(logging.WARNING)
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
