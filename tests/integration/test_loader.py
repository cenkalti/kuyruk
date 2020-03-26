import os
import sys
import json
import unittest

from what import What

from kuyruk import Kuyruk, Config

config = Config()
config.from_pyfile('/tmp/kuyruk_config.py')


class LoaderTestCase(unittest.TestCase):

    def test_load_single_file(self):
        self._test_function_name(
            'onefile.py',
            'loader',
            'onefile.print_message',
        )

    def test_load_directory(self):
        self._test_function_name(
            'main.py',
            'loader/appdirectory',
            'tasks.print_message',
        )

    def test_load_package(self):
        self._test_function_name(
            '-m apppackage.main',
            'loader',
            'apppackage.tasks.print_message',
        )

    def test_script_in_package(self):
        self._test_function_name(
            '-m apppackage.scripts.send_message',
            'loader',
            'apppackage.tasks.print_message',
        )

    def _test_function_name(self, args, cwd, name):
        with Kuyruk(config=config).channel() as ch:
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
    with Kuyruk(config=config).channel() as ch:
        message = ch.basic_get("kuyruk")
        desc = json.loads(message.body)
        return '.'.join([desc['module'], desc['function']])
