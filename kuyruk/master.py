import os
import sys
import signal
import socket
import logging
import itertools
import multiprocessing
from time import time, sleep

from setproctitle import setproctitle

from .worker import Worker

logger = logging.getLogger(__name__)


class Master(object):
    """
    Master worker implementation that coordinates queue workers.

    """
    def __init__(self, config):
        self.config = config
        self.workers = []
        self.shutdown_pending = False

    def run(self, queues=None):
        setproctitle('kuyruk: master')
        self._register_signals()

        if self.config.MAX_LOAD is None:
            self.config.MAX_LOAD = multiprocessing.cpu_count()

        if queues is None:
            try:
                queues = self.config.WORKERS[socket.gethostname()]
            except KeyError:
                logger.warning(
                    'No queues specified. Listening on default queue: "kuyruk"')
                queues = 'kuyruk'

        queues = parse_queues_str(queues)
        self._start_workers(queues)
        self._wait_for_workers()
        logger.info('End run master')

    def stop_workers(self):
        """Stop all running workers and exit."""
        for worker in self.workers:
            os.kill(worker.pid, signal.SIGTERM)

    def kill_workers(self):
        """Kill workers without waiting their tasks to finish."""
        for worker in self.workers:
            os.kill(worker.pid, signal.SIGKILL)

    def _start_workers(self, queues):
        """Start a new worker for each queue name"""
        logger.info('Starting to work on queues: %s', queues)
        for queue in queues:
            worker = Worker(queue, self.config)
            worker.start()
            self.workers.append(worker)

    def _spawn_new_worker(self, worker):
        """Spawn a new process with parameters same as the old worker."""
        logger.debug("Spawning new worker")
        new_worker = Worker(worker.queue_name, self.config)
        new_worker.start()
        self.workers.append(new_worker)
        self.workers.remove(worker)
        logger.debug(self.workers)

    def _wait_for_workers(self):
        """Loop until any of the self.workers is alive.
        If a worker is dead and Kuyruk is running state, spawn a new worker.

        """
        start = time()
        any_alive = True
        while any_alive:
            any_alive = False
            for worker in list(self.workers):
                if worker.is_alive():
                    any_alive = True
                else:
                    if not self.shutdown_pending:
                        self._spawn_new_worker(worker)
                        any_alive = True

            if any_alive:
                logger.debug("Waiting for workers... "
                             "%i seconds passed" % (time() - start))
                sleep(1)

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGHUP, self._handle_sighup)

    def _handle_sigint(self, signum, frame):
        if self.shutdown_pending:
            logger.warning("Cold shutdown")
            self.kill_workers()
            sys.exit(1)
        else:
            logger.warning("Warm shutdown")
            self._handle_sigterm(None, None)
        logger.debug("Handled SIGINT")

    def _handle_sigterm(self, signum, frame):
        self.stop_workers()
        self.shutdown_pending = True

    def _handle_sighup(self, signum, frame):
        if self.config.path:
            self.config.reload()
        self.stop_workers()


def parse_queues_str(s):
    """Parse command line queues string.

    :param s: Command line or configuration queues string
    :return: list of queue names
    """
    queues = (q.strip() for q in s.split(','))
    queues = itertools.chain.from_iterable(parse_count(q) for q in queues)
    return [parse_local(q) for q in queues]


def parse_count(q):
    parts = q.split('*', 1)
    if len(parts) > 1:
        return int(parts[0]) * [parts[1]]
    return [parts[0]]


def parse_local(q):
    if q.startswith('@'):
        return "%s_%s" % (q[1:], socket.gethostname())
    return q
