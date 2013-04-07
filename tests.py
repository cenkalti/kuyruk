#!/usr/bin/env python
import os
import sys
import time
import signal
import logging
import unittest
import threading
import traceback
import subprocess
from Queue import LifoQueue

from scripttest import TestFileEnvironment

from kuyruk import Kuyruk, Queue, Task
from kuyruk.connection import LazyConnection, LazyChannel

logger = logging.getLogger(__name__)

kuyruk = Kuyruk()


# These 2 functions below needs to be at module level in order that Kuyruk
# to determine their fully qualified name.
@kuyruk.task
def print_task(message):
    print message


@kuyruk.task(queue='another_queue')
def print_task2(message):
    print message


def run_kuyruk(queues='kuyruk'):
    env = TestFileEnvironment()
    out = LifoQueue()

    def target():
        result = env.run(
            sys.executable,
            '-m', 'kuyruk',
            '--queues', queues,
            cwd='/Users/cenk/projects/kuyruk',
            expect_stderr=True
        )
        out.put(result)
    t = threading.Thread(target=target)
    t.start()
    time.sleep(1)
    kill_cmd('kuyruk')
    time.sleep(1)
    return out.get()


class KuyrukTestCase(unittest.TestCase):

    def clear_queue(self, queue_name):
        conn = LazyConnection()
        ch = LazyChannel(conn)
        Queue(queue_name, ch).delete()
        ch.close()
        conn.close()

    def test_task_decorator(self):
        # Decorator without args
        self.assertTrue(isinstance(print_task, Task))
        # Decorator with args
        self.assertTrue(isinstance(print_task2, Task))

    def test_simple_task(self):
        self.clear_queue('kuyruk')
        print_task('hello world')

        result = run_kuyruk()
        assert 'hello world' in result.stdout

    def test_another_queue(self):
        self.clear_queue('another_queue')
        print_task2('hello another')

        result = run_kuyruk(queues='another_queue')
        assert 'hello another' in result.stdout


def kill_cmd(cmd):
    logger.debug('kill_cmd: %s', cmd)
    pids = get_pids(cmd)
    kill_pids(pids)


def get_pids(pattern):
    logger.debug('get_pids: %s', pattern)
    cmd = "pgrep -f '%s'" % pattern
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    pids = p.communicate()[0].split()
    logger.debug('pids: %s', pids)
    return map(int, pids)


def kill_pids(pids, signum=signal.SIGTERM):
    logger.debug('kill_pids: %s', pids)
    for pid in pids:
        logger.info("pid %s is alive, sending %s", pid, signum)
        try:
            logger.debug('killing %s', pid)
            os.kill(pid, signum)
        except OSError:
            traceback.print_exc()


if __name__ == '__main__':
    logging.getLogger('pika').setLevel(logging.WARNING)
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
