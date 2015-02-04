from __future__ import print_function
import os
import sys
import errno
import logging
import subprocess
from time import time, sleep
from functools import partial
from contextlib import contextmanager

from what import What

from kuyruk import Kuyruk


if os.environ.get('TRAVIS', '') == 'true':
    TIMEOUT = 30
else:
    TIMEOUT = 3

logger = logging.getLogger(__name__)


def delete_queue(*queues):
    """Delete queues from RabbitMQ"""
    k = Kuyruk()
    with k.connection() as conn:
        for name in queues:
            logger.debug("deleting queue %s", name)
            with conn.channel() as ch:
                ch.queue_delete(queue=name)


def len_queue(queue):
    with Kuyruk().channel() as ch:
        _, count, _ = ch.queue_declare(queue=queue, durable=True,
                                       passive=True, auto_delete=False)
        return count


def is_empty(queue):
    return len_queue(queue) == 0


@contextmanager
def run_kuyruk(queue='kuyruk', terminate=True):
    assert not_running()
    args = [
        sys.executable, '-u',
        '-m', 'kuyruk.__main__',  # run main module
        "--app", "tests.tasks.kuyruk",
        "worker", '--queue', queue,
    ]

    environ = os.environ.copy()
    environ['COVERAGE_PROCESS_START'] = '.coveragerc'

    popen = What(*args, preexec_fn=os.setsid, env=environ)
    popen.timeout = TIMEOUT
    try:
        yield popen

        if terminate:
            # Send SIGTERM to worker for gracefull shutdown
            popen.terminate()
            popen.expect("End run worker")

        popen.expect_exit()

    finally:
        # We need to make sure that not any process of kuyruk is running
        # after the test is finished.

        # Kill the process and wait until it is dead
        try:
            popen.kill()
            popen.wait()
        except OSError as e:
            if e.errno != errno.ESRCH:  # No such process
                raise

        logger.debug('Worker return code: %s', popen.returncode)

        try:
            wait_while(lambda: get_pids('kuyruk:'))
        except KeyboardInterrupt:
            print(popen.get_output())
            # Do not raise KeyboardInterrupt here because nose does not print
            # captured stdout and logging on KeyboardInterrupt
            raise Exception


def not_running():
    return not is_running()


def is_running():
    return bool(get_pids('kuyruk:'))


def get_pids(pattern):
    logger.debug('get_pids: %s', pattern)
    cmd = "pgrep -fl '%s'" % pattern
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    out = p.communicate()[0]
    logger.debug("\n%s", out)
    lines = out.splitlines()
    lines = [l.decode() for l in lines]
    lines = [l.split(" ", 1) for l in lines]
    lines = [(pid, cmd) for (pid, cmd) in lines if not cmd.startswith(("sh", "/bin/sh"))]
    pids = [int(pid) for (pid, cmd) in lines]
    logger.debug('pids: %s', pids)
    return pids


def get_pid(pattern):
    pids = get_pids(pattern)
    assert len(pids) == 1
    return pids[0]


def wait_until(f, timeout=None):
    return wait_while(lambda: not f(), timeout)


def wait_while(f, timeout=None):
    do_while(partial(sleep, 0.1), f, timeout)


def do_until(f_do, f_cond, timeout=None):
    do_while(f_do, lambda: not f_cond(), timeout)


def do_while(f_do, f_condition, timeout=None):
    def should_do():
        if timeout and timeout < 0:
            raise Timeout
        return f_condition()

    original_timeout = timeout
    start = time()
    while should_do():
        f_do()
        if timeout:
            passed = time() - start
            timeout = original_timeout - passed


class Timeout(Exception):
    pass
