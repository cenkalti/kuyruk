import os
import sys
import json

from what import What
from tests.integration.util import amqp_channel


class TestLoader:

    def test_load_single_file(self, rabbitmq):
        self._test_function_name(
            rabbitmq,
            'onefile.py',
            'loader',
            'onefile.print_message',
        )

    def test_load_directory(self, rabbitmq):
        self._test_function_name(
            rabbitmq,
            'main.py',
            'loader/appdirectory',
            'tasks.print_message',
        )

    def test_load_package(self, rabbitmq):
        self._test_function_name(
            rabbitmq,
            '-m apppackage.main',
            'loader',
            'apppackage.tasks.print_message',
        )

    def test_script_in_package(self, rabbitmq):
        self._test_function_name(
            rabbitmq,
            '-m apppackage.scripts.send_message',
            'loader',
            'apppackage.tasks.print_message',
        )

    def _test_function_name(self, rabbitmq, args, cwd, name):
        with amqp_channel(rabbitmq) as ch:
            print(cwd, args, name)

            ch.queue_delete("kuyruk")

            # Every call sends a task to the queue
            run_python(args, cwd=cwd)

            # Can we load the task by name?
            got = get_name(ch)
            assert got == name, got


def run_python(args, cwd):
    dirname = os.path.dirname(__file__)
    cwd = os.path.join(dirname, cwd)
    What(sys.executable, *args.split(' '), cwd=cwd).expect_exit(0)


def get_name(channel):
    message = channel.basic_get("kuyruk")
    desc = json.loads(message.body)
    return '.'.join([desc['module'], desc['function']])
