import os
import sys
import signal
import logging
import subprocess
from time import sleep
from functools import wraps
from contextlib import contextmanager

import pexpect

from ..connection import LazyConnection
from ..queue import Queue as RabbitQueue

logger = logging.getLogger(__name__)


def clear(*queues):
    """Decorator for deleting queue from RabbitMQ"""
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
            delete_queue(*queues)
            return f(*args, **kwargs)
        return inner
    return decorator


def delete_queue(*queues):
    """Delete queues from RabbitMQ"""
    conn = LazyConnection()
    ch = conn.channel()
    with conn:
        with ch:
            for name in queues:
                RabbitQueue(name, ch).delete()


def is_empty(queue):
    queue = RabbitQueue(queue, LazyConnection().channel())
    return len(queue) == 0


@contextmanager
def run_kuyruk(queues='kuyruk'):
    # ensure_not_running('kuyruk:')
    child = pexpect.spawn(sys.executable, [
        '-m', 'kuyruk.__main__',  # run main module
        '--queues', queues,
    ], timeout=10)
    yield child
    child.close(force=True)


def ensure_not_running(pattern):
    logger.debug('Ensuring cmd pattern: "%s" is not running', pattern)
    pids = get_pids(pattern)
    logger.debug("pids: %s", pids)
    for pid in pids:
        kill_pid(pid, signum=signal.SIGKILL)
    sleep(0.1)
    pids = get_pids(pattern)
    logger.debug("pids after kill: %s", pids)
    assert not pids


def kill_kuyruk(signum=signal.SIGTERM, worker='master'):
    pids = get_pids('kuyruk: %s' % worker)
    assert len(pids) == 1, pids
    kill_pid(pids[0], signum=signum)


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


def kill_pid(pid, signum=signal.SIGTERM):
    try:
        logger.debug('killing %s', pid)
        os.kill(pid, signum)
    except OSError as e:
        if e.errno != 3:  # No such process
            raise
