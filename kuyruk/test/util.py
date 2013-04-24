import os
import sys
import signal
import logging
import subprocess
from time import time, sleep
from functools import partial, wraps
from contextlib import contextmanager

import pexpect
from nose.plugins.skip import SkipTest

from ..channel import LazyChannel
from ..queue import Queue as RabbitQueue

TIMEOUT = 10

logger = logging.getLogger(__name__)


def delete_queue(*queues):
    """Delete queues from RabbitMQ"""
    ch = LazyChannel()
    with ch:
        for name in queues:
            RabbitQueue(name, ch).delete()


def is_empty(queue):
    ch = LazyChannel()
    with ch:
        queue = RabbitQueue(queue, ch)
        return len(queue) == 0


def skip_on_travis(f):
    """Some process related tests are failing on Travis-CI"""
    @wraps(f)
    def inner(*args, **kwargs):
        if os.environ.get('TRAVIS') == 'true':
            raise SkipTest
        return f(*args, **kwargs)
    return inner


@contextmanager
def run_kuyruk(queues=None, save_failed_tasks=False, terminate=True):
    assert not_running()
    args = ['-m', 'kuyruk.__main__']  # run main module
    args.extend(['--logging-level=DEBUG'])
    if queues:
        args.extend(['--queues', queues])

    if save_failed_tasks:
        args.append('--save-failed-tasks')

    child = pexpect.spawn(sys.executable, args, timeout=TIMEOUT)
    try:
        yield child
        # Master and workers should exit normally after SIGTERM
        if terminate:
            # Try to terminate kuyruk gracefully
            child.kill(signal.SIGTERM)
            child.expect('End run master', timeout=TIMEOUT)
            child.close()
        sleep_until(not_running, timeout=TIMEOUT)
    finally:
        # We need to make sure that not any process of kuyruk running
        def kill():
            kill_all(signal.SIGKILL)
            sleep(0.25)
        child.close(force=True)
        do_until(kill, not_running)


def not_running():
    return not is_running()


def is_running():
    return get_pids('kuyruk:')


def run_requeue():
    pexpect.run('%s -m kuyruk.requeue' % sys.executable)


def kill_worker(signum=signal.SIGTERM):
    pkill('kuyruk: worker', signum)


def kill_master(signum=signal.SIGTERM):
    pkill('kuyruk: master', signum)


def kill_all(signum=signal.SIGTERM):
    pkill('kuyruk:', signum)


def pkill(pattern, signum=signal.SIGTERM):
    logger.info("Killing pattern: '%s' with signal: %s" % (pattern, signum))
    pexpect.run("pkill -%i -f '%s'" % (signum, pattern))


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

    start = time()
    while should_do():
        f_do()
        if timeout:
            passed = time() - start
            timeout -= passed


class Timeout(Exception):
    pass
