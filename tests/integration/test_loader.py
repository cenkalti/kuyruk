from __future__ import print_function
import os
import sys
import json
import unittest

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
        with Kuyruk().channel() as ch:
            for args, cwd, name in cases:
                print(cwd, args, name)

                ch.queue_delete("kuyruk")

                # Every call sends a task to the queue
                run_python(args, cwd=cwd)

                # Can we load the task by name?
                got = get_name()
                assert got == name, got


def run_python(args, cwd):
    dirname = os.path.dirname(__file__)
    cwd = os.path.join(dirname, cwd)
    What(sys.executable, *args.split(' '), cwd=cwd).expect_exit(0)


def get_name():
    with Kuyruk().channel() as ch:
        message = ch.basic_get("kuyruk")
        desc = json.loads(message.body)
        return '.'.join([desc['module'], desc['function']])
