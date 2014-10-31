import os
import sys
import json
import unittest

import rabbitpy
from what import What

from kuyruk import Kuyruk


class LoaderTestCase(unittest.TestCase):

    def test_function_name(self):
        cases = [
            (
                'onefile.py',
                'loader',
                'onefile.print_message'
            ),
            (
                'main.py',
                'loader/appdirectory',
                'tasks.print_message'
            ),
            (
                '-m apppackage.main',
                'loader',
                'apppackage.tasks.print_message'
            ),
            (
                '-m apppackage.scripts.send_message',
                'loader',
                'apppackage.tasks.print_message'
            ),
        ]
        k = Kuyruk()
        with k.connection as conn:
            with conn.channel() as ch:
                for args, cwd, name in cases:
                    print cwd, args, name

                    queue = rabbitpy.Queue(ch, "kuyruk", durable=True)
                    queue.delete()

                    # Every call sends a task to the queue
                    run_python(args, cwd=cwd)

                    # Can we load the task by name?
                    assert get_task_name(queue) == name


def run_python(args, cwd):
    dirname = os.path.dirname(__file__)
    cwd = os.path.join(dirname, cwd)
    What(sys.executable, *args.split(' '), cwd=cwd).expect_exit(0)


def get_task_name(queue):
    m = queue.get()
    d = json.loads(m.body)
    return '.'.join([d['module'], d['function']])
