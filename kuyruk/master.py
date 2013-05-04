from __future__ import absolute_import
import os
import sys
import signal
import socket
import logging
import itertools
import threading
import multiprocessing
from time import time, sleep

from setproctitle import setproctitle

from kuyruk.worker import Worker
from kuyruk.manager.client import ManagerClientMixin

logger = logging.getLogger(__name__)


class Master(multiprocessing.Process, ManagerClientMixin):
    """
    Master worker implementation that coordinates queue workers.

    """
    def __init__(self, config):
        super(Master, self).__init__()
        self.config = config
        if self.config.MAX_LOAD is None:
            self.config.MAX_LOAD = multiprocessing.cpu_count()

        self.workers = []
        self.shutdown_pending = threading.Event()
        self.override_queues = None

    def run(self):
        logger.debug('Process id: %s', os.getpid())
        logger.debug('Process group id: %s', os.getpgrp())
        setproctitle('kuyruk: master')
        self._register_signals()
        self.started = time()
        self.start_manager_client(
            self.config.MANAGER_HOST,
            self.config.MANAGER_PORT,
            self.shutdown_pending)
        self._start_workers()
        self._wait_for_workers()
        logger.debug('End run master')

    def _start_workers(self):
        """Start a new worker for each queue"""
        queues = self._get_queues()
        queues = parse_queues_str(queues)
        logger.info('Starting to work on queues: %s', queues)
        for queue in queues:
            worker = Worker(queue, self.config)
            worker.start()
            self.workers.append(worker)

    def _get_queues(self):
        """Return queues string."""
        if self.override_queues:
            return self.override_queues

        hostname = socket.gethostname()
        try:
            return self.config.WORKERS[hostname]
        except KeyError:
            logger.warning('No queues specified for host %r. '
                           'Listening on default queue: "kuyruk"', hostname)
            return 'kuyruk'

    def stop_workers(self, workers=None, kill=False):
        """Send stop signal to all workers."""
        if workers is None:
            workers = self.workers

        for worker in workers:
            os.kill(worker.pid, signal.SIGKILL if kill else signal.SIGTERM)

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
                elif not self.shutdown_pending.is_set():
                    self._kill_pg(worker)
                    self._spawn_new_worker(worker)
                    any_alive = True
                else:
                    self.workers.remove(worker)

            if any_alive:
                logger.debug("Waiting for workers... "
                             "%i seconds passed" % (time() - start))
                sleep(1)

    def _spawn_new_worker(self, worker):
        """Spawn a new process with parameters same as the old worker."""
        logger.debug("Spawning new worker")
        new_worker = Worker(worker.queue_name, self.config)
        new_worker.start()
        self.workers.append(new_worker)
        self.workers.remove(worker)
        logger.debug(self.workers)

    def _kill_pg(self, worker):
        try:
            # Worker could spawn processes in task,
            # kill them all.
            os.killpg(worker.pid, signal.SIGKILL)
        except OSError as e:
            if e.errno != 3:  # No such process
                raise

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGHUP, self._handle_sighup)
        signal.signal(signal.SIGQUIT, self._handle_sigquit)

    def _handle_sigterm(self, signum, frame):
        logger.warning("Handling SIGTERM")
        self.warm_shutdown()

    def _handle_sigquit(self, signum, frame):
        logger.warning("Handling SIGQUIT")
        self.cold_shutdown()

    def _handle_sigint(self, signum, frame):
        logger.warning("Handling SIGINT")
        if sys.stdin.isatty() and not self.shutdown_pending.is_set():
            self.warm_shutdown()
        else:
            self.cold_shutdown()
        logger.debug("Handled SIGINT")

    def _handle_sighup(self, signum, frame):
        logger.warning("Handling SIGHUP")
        self.reload()

    def warm_shutdown(self):
        logger.warning("Warm shutdown")
        self.shutdown_pending.set()
        self.stop_workers()

    def cold_shutdown(self):
        logger.warning("Cold shutdown")
        self.shutdown_pending.set()
        self.stop_workers(kill=True)

    def reload(self):
        logger.warning("Reloading workers")
        old_workers = self.workers
        if hasattr(self.config, 'path'):
            self.config.reload()
            self.workers = []
            self._start_workers()
        self.stop_workers(workers=old_workers)

    def generate_message(self):
        """Generate stats to be sent to manager."""
        return {
            'type': 'master',
            'hostname': socket.gethostname(),
            'uptime': self.uptime,
            'pid': os.getpid(),
            'load': os.getloadavg(),
        }

    @property
    def uptime(self):
        return int(time() - self.started)


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
