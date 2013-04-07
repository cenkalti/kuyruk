import os
import signal
import logging
import traceback
import subprocess

logger = logging.getLogger(__name__)


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
