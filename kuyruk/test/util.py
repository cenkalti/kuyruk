import os
import sys
import signal
import logging
import subprocess
from time import time, sleep
from functools import partial
from contextlib import contextmanager

from what import What

from ..channel import LazyChannel
from ..queue import Queue as RabbitQueue

TIMEOUT = 5

logger = logging.getLogger(__name__)


def delete_queue(*queues):
    """Delete queues from RabbitMQ"""
    with LazyChannel() as channel:
        for name in queues:
            RabbitQueue(name, channel).delete()


def is_empty(queue):
    with LazyChannel() as channel:
        queue = RabbitQueue(queue, channel)
        return len(queue) == 0


@contextmanager
def run_kuyruk(queues=None, save_failed_tasks=False, terminate=True):
    assert not_running()
    args = [
        sys.executable, '-u',
        '-m', 'kuyruk.__main__',  # run main module
        '--max-load', '999',
    ]
    args.extend(['--logging-level=DEBUG'])
    if queues:
        args.extend(['--queues', queues])

    if save_failed_tasks:
        args.append('--save-failed-tasks')

    child = What(*args)
    child.timeout = TIMEOUT
    try:
        yield child
        # Master and workers should exit normally after SIGTERM
        if terminate:
            # Try to terminate kuyruk gracefully
            child.terminate()
            child.expect('End run master')
        sleep_until(not_running, timeout=TIMEOUT)
    finally:
        # We need to make sure that not any process of kuyruk running
        get_worker_pids = lambda: get_pids('kuyruk: worker')
        get_master_pid = lambda: get_pids('kuyruk: master')

        # Kill master and wait until it is dead
        try:
            logger.info('Killing process group: %s', child.pid)
            os.killpg(child.pid, signal.SIGTERM)
        except OSError:
            pass
        else:
            logger.debug('Child return code: %s', child.returncode)
            sleep_while(get_master_pid)

        # Kill all running workers and wait until they are dead
        for pid in get_worker_pids():
            os.kill(pid, signal.SIGKILL)
        sleep_while(get_worker_pids)


def not_running():
    return not is_running()


def is_running():
    return bool(get_pids('kuyruk:'))


def run_requeue():
    w = What(sys.executable, '-u', '-m', 'kuyruk.requeue')
    w.expect_exit(0, TIMEOUT)


def pkill(pattern, signum=signal.SIGTERM):
    logger.info("Killing pattern: '%s' with signal: %s" % (pattern, signum))
    What('pkill', '-%i' % signum, '-f', "'%s'" % pattern).wait()


def get_pids(pattern):
    logger.debug('get_pids: %s', pattern)
    cmd = "pgrep -fl '%s'" % pattern
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    out = p.communicate()[0]
    logger.debug("\n%s", out)
    lines = out.splitlines()
    # filter pgrep itself. travis runs it like: "/bin/sh -c pgrep -fl 'kuyruk:'"
    lines = filter(lambda x: not cmd in x, lines)
    pids = [int(l.split()[0]) for l in lines]  # take first column
    logger.debug('pids: %s', pids)
    return pids


def get_pid(pattern):
    pids = get_pids(pattern)
    assert len(pids) == 1
    return pids[0]


def sleep_until(f, timeout=None):
    return sleep_while(lambda: not f(), timeout)


def sleep_while(f, timeout=None):
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
