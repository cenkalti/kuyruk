import os
import sys
import signal
import logging
import traceback
import threading
import subprocess
from time import sleep
from Queue import Queue
from functools import wraps

from scripttest import TestFileEnvironment

from ..connection import LazyConnection, LazyChannel
from ..queue import Queue as RabbitQueue

logger = logging.getLogger(__name__)


def clear(*queues):
    """Decorator for deleting queue from RabbitMQ"""
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
            conn = LazyConnection()
            ch = conn.channel()
            with conn:
                with ch:
                    for name in queues:
                        RabbitQueue(name, ch).delete()
            return f(*args, **kwargs)
        return inner
    return decorator


def run_kuyruk(
        queues='kuyruk',
        signum=signal.SIGTERM,
        expect_error=False,
        seconds=1,
        cold_shutdown=False):
    def target():
        result = env.run(
            sys.executable,
            '-m', 'kuyruk.__main__',  # run main module
            '--queues', queues,
            expect_stderr=True,  # logging output goes to stderr
            expect_error=expect_error)
        out.put(result)
    env = TestFileEnvironment()
    out = Queue()
    t = threading.Thread(target=target)
    t.start()
    sleep(seconds)
    kill_kuyruk(signum=signum)
    if cold_shutdown:
        kill_kuyruk(signum=signum)
    return out.get(timeout=2)


def kill_kuyruk(signum=signal.SIGTERM):
    pids = get_pids('kuyruk.__main__')
    pid = min(pids)  # select master
    kill_pid(pid, signum=signum)


def get_pids(pattern):
    logger.debug('get_pids: %s', pattern)
    cmd = "pgrep -f '%s'" % pattern
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    pids = p.communicate()[0].split()
    logger.debug('pids: %s', pids)
    return map(int, pids)


def kill_pid(pid, signum=signal.SIGTERM):
    try:
        logger.debug('killing %s', pid)
        os.kill(pid, signum)
    except OSError:
        traceback.print_exc()
