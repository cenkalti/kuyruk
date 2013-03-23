#!/usr/bin/env python
import os
import sys
import logging
import unittest

# HACK: Prepend ../ to PYTHONPATH so that we can import kuyruk form there.
TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.realpath(os.path.join(TESTS_ROOT, '..')))

from kuyruk import Kuyruk, Task, Queue

kuyruk = Kuyruk()
kuyruk2 = Kuyruk()
called = False


@kuyruk.task
def print_task(message):
    global called
    called = True
    print message


@kuyruk2.task(queue='another_queue')
def print_task2(message):
    global called
    called = True
    print message


def run_kuyruk(kuyruk, max_tasks=1):
    kuyruk.max_tasks = max_tasks
    kuyruk.run()


class KuyrukTestCase(unittest.TestCase):

    def setUp(self):
        global called
        called = False

        Queue('kuyruk', kuyruk.connection).delete()

    def test_task_decorator(self):
        self.assertIsInstance(print_task, Task)
        self.assertIsInstance(print_task2, Task)

    def test_simple_task(self):
        print_task('hello world')
        run_kuyruk(kuyruk)
        self.assertEqual(called, True)

    def test_another_queue(self):
        Queue('another_queue', kuyruk2.connection).delete()

        print_task2('hello world')
        kuyruk2.queue = 'another_queue'
        run_kuyruk(kuyruk2)
        self.assertEqual(called, True)


if __name__ == '__main__':
    logging.getLogger('pika').setLevel(logging.WARNING)
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
