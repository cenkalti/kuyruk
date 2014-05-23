from __future__ import absolute_import
import os
import sys
import errno
import signal
import socket
import string
import logging
import resource
import itertools
import threading
from time import sleep
from collections import namedtuple

import rpyc
from setproctitle import setproctitle

from kuyruk import __version__, helpers
from kuyruk.process import KuyrukProcess


logger = logging.getLogger(__name__)


class Master(KuyrukProcess):
    """Spawn multiple workers and supervise them by reading the list of queues
    from configuration.

    """
    def __init__(self, config):
        super(Master, self).__init__(config)
        self.workers = {}
        self.lock = threading.Lock()

    def run(self):
        super(Master, self).run()
        setproctitle('kuyruk: master')
        self.maybe_start_manager_rpc_service()
        self.start_workers()
        self.wait_for_workers()
        logger.debug('End run master')

    def rpc_service_class(self):
        class _Service(rpyc.Service):
            exposed_get_stats = self.get_stats
            exposed_warm_shutdown = self.warm_shutdown
            exposed_cold_shutdown = self.cold_shutdown
            exposed_abort = self.abort
        return _Service

    def start_workers(self):
        """Start a new worker for each queue."""
        queues = self.get_queues()
        queues = parse_queues_str(queues)
        logger.info('Starting to work on queues: %s', queues)
        for queue in queues:
            self.create_new_worker(queue)

    def get_queues(self):
        """Return queues string by reading from configuration."""
        if isinstance(self.config.QUEUES, basestring):
            return self.config.QUEUES

        hostname = socket.gethostname()
        try:
            return self.config.QUEUES[hostname]
        except KeyError:
            logger.warning(
                'No queues specified for host %r. '
                'Listening on default queues.', hostname)
            return self.config.DEFAULT_QUEUES

    def shutdown_workers(self, kill=False):
        """Send shutdown signal to all workers."""
        if kill:
            fn, sig = os.killpg, signal.SIGKILL
        else:
            fn, sig = os.kill, signal.SIGTERM

        for worker in self.workers.itervalues():
            try:
                logger.info("Sending signal %s to PID %s", sig, worker.pid)
                fn(worker.pid, sig)
            except OSError as e:
                if e.errno == errno.ESRCH:
                    # Worker may be restarting and dead
                    logger.warning("Cannot find PID %s", worker.pid)
                else:
                    raise

    def wait_for_workers(self):
        """Loop until any of the self.workers is alive.
        If a worker is dead and Kuyruk is running state, spawns a new worker.

        """
        retry_wait = helpers.retry_on_eintr(os.wait)
        while self.workers:
            pid, status = retry_wait()
            logger.info("Worker: %s is dead", pid)
            worker = self.workers[pid]
            worker.kill_pg()
            del self.workers[pid]
            if not self.shutdown_pending.is_set():
                self.respawn_worker(worker)
                sleep(1)  # Prevent cpu burning in case a worker cant start

    def respawn_worker(self, worker):
        """Spawn a new process with parameters same as the old worker."""
        logger.warning("Respawning worker %s", worker)
        self.create_new_worker(worker.queue)
        logger.debug(self.workers)

    def create_new_worker(self, queue):
        if self.shutdown_pending.is_set():
            logger.info("Shutdown is pending. Skipped spawning new worker.")
            return
        worker = WorkerProcess(self.kuyruk, queue)
        with self.lock:
            worker.start()
        self.workers[worker.pid] = worker

    def register_signals(self):
        super(Master, self).register_signals()
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGQUIT, self.handle_sigquit)
        signal.signal(signal.SIGABRT, self.handle_sigabrt)

    def handle_sigterm(self, signum, frame):
        logger.warning("Handling SIGTERM")
        self.warm_shutdown()

    def handle_sigquit(self, signum, frame):
        logger.warning("Handling SIGQUIT")
        self.cold_shutdown()

    def handle_sigabrt(self, signum, frame):
        logger.warning("Handling SIGABRT")
        self.abort()

    def warm_shutdown(self):
        logger.warning("Warm shutdown")
        self.shutdown_pending.set()
        self.shutdown_workers()

    def cold_shutdown(self):
        logger.warning("Cold shutdown")
        self.shutdown_pending.set()
        self.shutdown_workers(kill=True)
        self._exit(0)

    def abort(self):
        """Exit immediately making workers orphan."""
        logger.warning("Aborting")
        self._exit(0)

    def get_stats(self):
        """Generate stats to be sent to manager."""
        return {
            'type': 'master',
            'hostname': socket.gethostname(),
            'uptime': self.uptime,
            'pid': os.getpid(),
            'version': __version__,
            'workers': len(self.workers),
            'load': os.getloadavg(),
        }


class WorkerProcess(object):

    def __init__(self, kuyruk, queue):
        self.kuyruk = kuyruk
        self.queue = queue
        self.pid = None

    def start(self):
        logger.debug("Forking...")
        pid = os.fork()
        if pid:
            # parent
            logger.debug("I am parent")
            self.pid = pid
        else:
            # child
            logger.debug("I am child")
            self.run_worker()

    def run_worker(self):
        logger.debug("Hello from worker")

        logger.debug("Setting new process group")
        os.setpgrp()

        self.close_fds()

        # Fake command line queue argument
        Args = namedtuple('Args', 'queue')
        args = Args(queue=self.queue)

        # This will run as if called "kuyruk worker" from command line
        from kuyruk.__main__ import run_worker
        logger.debug("Running worker command")
        run_worker(self.kuyruk, args)

        sys.exit(0)

    def close_fds(self):
        logger.debug("Closing open file descriptors...")
        maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
        if maxfd == resource.RLIM_INFINITY:
            maxfd = 1024

        # Do not close stdin, stdout and stderr (0, 1, 2)
        os.closerange(3, maxfd)

    def kill_pg(self):
        """Kill the process with their children. Does not raise exception
        if the process is not alive.

        """
        logger.debug("Killing worker's process group with SIGKILL")
        try:
            os.killpg(self.pid, signal.SIGKILL)
        except OSError as e:
            if e.errno != errno.ESRCH:  # No such process
                raise


def parse_queues_str(s):
    """Parse command line queues string.

    :param s: Command line or configuration queues string
    :return: list of queue names
    """
    queues = (q.strip() for q in s.split(','))
    return list(itertools.chain.from_iterable(expand_count(q) for q in queues))


def expand_count(q):
    parts = q.split('*', 1)
    parts = map(string.strip, parts)
    if len(parts) > 1:
        try:
            return int(parts[0]) * [parts[1]]
        except ValueError:
            return int(parts[1]) * [parts[0]]
    return [parts[0]]
