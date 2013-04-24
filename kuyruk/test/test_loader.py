import os
import sys
import unittest

import pexpect

from ..queue import Queue
from ..channel import LazyChannel
from util import delete_queue


class LoaderTestCase(unittest.TestCase):

    def test_function_name(self):
        self._test_function_name(
            'onefile.py',
            'loader',
            'onefile.print_message'
        )
        self._test_function_name(
            'main.py',
            'loader/appdirectory',
            'tasks.print_message'
        )
        self._test_function_name(
            '-m apppackage.main',
            'loader',
            'apppackage.tasks.print_message'
        )
        self._test_function_name(
            '-m apppackage.scripts.send_message',
            'loader',
            'apppackage.tasks.print_message'
        )

    def _test_function_name(self, args, cwd, name):
        delete_queue('kuyruk')
        run_python(args, cwd=cwd)
        assert_name(name)


def run_python(args, cwd):
    dirname = os.path.dirname(__file__)
    cwd = os.path.join(dirname, cwd)
    command = "%s %s" % (sys.executable, args)
    print pexpect.run(command, cwd=cwd)


def assert_name(name):
    f = get_name()
    assert f == name, "%s != %s" % (f, name)


def get_name():
    ch = LazyChannel()
    with ch:
        desc = Queue('kuyruk', ch).receive()[1]
        return '.'.join([desc['module'], desc['function']])
