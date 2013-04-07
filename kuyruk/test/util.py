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


def run_kuyruk(queues='kuyruk', signum=signal.SIGTERM, expect_error=False):
    def target():
        result = env.run(
            sys.executable,
            '-m', 'kuyruk.__main__',  # run main module
            '--queues', queues,
            expect_stderr=True,  # logging output goes to stderr
            expect_error=expect_error,
            )
        out.put(result)
    env = TestFileEnvironment()
    out = Queue()
    t = threading.Thread(target=target)
    t.start()
    sleep(1)
    kill_cmd('kuyruk.__main__', signum=signum)
    sleep(1)
    return out.get()


def kill_cmd(cmd, signum=signal.SIGTERM):
    logger.debug('kill_cmd: %s', cmd)
    pids = get_pids(cmd)
    kill_pids(pids, signum=signum)


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
