from __future__ import absolute_import
import os
import sys
import signal
import socket
import logging
import itertools
import multiprocessing
from time import time, sleep
from setproctitle import setproctitle
from kuyruk.worker import Worker
from kuyruk.process import Process
from kuyruk.version import __version__

logger = logging.getLogger(__name__)


class Master(Process):
    """
    Master worker implementation that coordinates queue workers.

    """
    def __init__(self, config):
        super(Master, self).__init__(config)
        self.workers = []
        self.override_queues = None
        if self.config.MAX_LOAD is None:
            self.config.MAX_LOAD = multiprocessing.cpu_count()

    def run(self):
        super(Master, self).run()
        setproctitle('kuyruk: master')
        self.start_workers()
        self.maybe_start_manager_thread()
        self.wait_for_workers()
        logger.debug('End run master')

    def start_workers(self):
        """Start a new worker for each queue"""
        queues = self.get_queues()
        queues = parse_queues_str(queues)
        logger.info('Starting to work on queues: %s', queues)
        for queue in queues:
            worker = Worker(queue, self.config)
            worker.start()
            self.workers.append(worker)

    def get_queues(self):
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

    def stop_workers(self, kill=False):
        """Send stop signal to all workers."""
        for worker in self.workers:
            os.kill(worker.pid, signal.SIGKILL if kill else signal.SIGTERM)

    def wait_for_workers(self):
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
                    worker.kill_pg()
                    self.spawn_new_worker(worker)
                    any_alive = True
                else:
                    self.workers.remove(worker)

            if any_alive:
                logger.debug("Waiting for workers... "
                             "%i seconds passed" % (time() - start))
                sleep(1)

    def spawn_new_worker(self, worker):
        """Spawn a new process with parameters same as the old worker."""
        logger.debug("Spawning new worker")
        new_worker = Worker(worker.queue_name, self.config)
        new_worker.start()
        self.workers.append(new_worker)
        self.workers.remove(worker)
        logger.debug(self.workers)

    def register_signals(self):
        signal.signal(signal.SIGINT, self.handle_sigint)
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

    def handle_sigint(self, signum, frame):
        logger.warning("Handling SIGINT")
        if sys.stdin.isatty() and not self.shutdown_pending.is_set():
            self.warm_shutdown()
        else:
            self.cold_shutdown()
        logger.debug("Handled SIGINT")

    def warm_shutdown(self):
        logger.warning("Warm shutdown")
        self.shutdown_pending.set()
        self.stop_workers()

    def cold_shutdown(self):
        logger.warning("Cold shutdown")
        self.shutdown_pending.set()
        self.stop_workers(kill=True)

    def abort(self):
        """Exit immediately making workers orphan."""
        logger.warning("Aborting")
        os._exit(1)

    def get_stats(self):
        """Generate stats to be sent to manager."""
        return {
            'type': 'master',
            'hostname': socket.gethostname(),
            'uptime': self.uptime,
            'pid': os.getpid(),
            'version': __version__,
            'load': os.getloadavg(),
        }


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
        try:
            return int(parts[0]) * [parts[1]]
        except ValueError:
            return int(parts[1]) * [parts[0]]
    return [parts[0]]


def parse_local(q):
    if q.startswith('@'):
        return "%s_%s" % (q[1:], socket.gethostname())
    return q
