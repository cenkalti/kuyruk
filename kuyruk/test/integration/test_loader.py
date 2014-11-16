import os
import sys
import json
import unittest

from what import What

from kuyruk import Kuyruk
from kuyruk.helpers.json_datetime import JSONDecoder
from kuyruk.test.integration.util import delete_queue


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
        for args, cwd, name in cases:
            print cwd, args, name
            delete_queue('kuyruk')
            run_python(args, cwd=cwd)  # Every call sends a task to the queue
            name_from_queue = get_name()
            assert name_from_queue == name  # Can we load the task by name?


def run_python(args, cwd):
    dirname = os.path.dirname(__file__)
    cwd = os.path.join(dirname, cwd)
    What(sys.executable, *args.split(' '), cwd=cwd).expect_exit(0)


def get_name():
    k = Kuyruk()
    try:
        with k.channel() as ch:
            message = ch.basic_get("kuyruk")
            desc = json.loads(message.body, cls=JSONDecoder)
            return '.'.join([desc['module'], desc['function']])
    finally:
        k.close()
