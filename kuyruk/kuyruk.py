import os
import sys
import time
import socket
import signal
import logging
import itertools
import multiprocessing

from setproctitle import setproctitle

from .task import Task
from .worker import Worker
from .config import Config

logger = logging.getLogger(__name__)


class Kuyruk(object):
    """
    Main class for Kuyruk distributed task queue. It holds the configuration
    values and provides a task decorator for user application and run method
    for workers.

    """

    def __init__(self, config_object={}):
        """
        :param config_object: See config.py for default values.

        """
        self.config = Config(config_object)
        self.workers = []
        self.pid = None
        self.stopping = False

    def task(self, queue='kuyruk', eager=False, retry=0):
        """Wrap functions with this decorator to convert them to background
        tasks. After wrapping, normal calls will send a message to queue
        instead of running the actual function.

        :param queue: Queue name for the tasks
        :param eager: Run task in process, do not use RabbitMQ
        :param retry: Retry this times before give up
        :return: Callable Task object wrapping the original function
        """
        def decorator():
            def inner(f):
                queue_ = 'kuyruk' if callable(queue) else queue
                return Task(f, self, queue=queue_, eager=eager, retry=retry)
            return inner

        if callable(queue):
            logger.debug('task without args')
            return decorator()(queue)
        else:
            logger.debug('task with args')
            return decorator()

    def run(self, queues=None):
        """Run Kuyruk workers.
        :param queues: queues str passed directly from command line
        :return: None

        This function may exit() before returning if SIGINT or SIGTERM
        received.

        """
        self.pid = os.getpid()
        self._register_signals()
        setproctitle('kuyruk: master')

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
        self.stopping = True
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
            worker = Worker(queue, self.config, self.pid)
            worker.start()
            self.workers.append(worker)

    def _spawn_new_worker(self, worker):
        """Spawn a new process with parameters same as the old worker."""
        logger.debug("Spawning new worker")
        new_worker = Worker(worker.queue_name, self.config, self.pid)
        new_worker.start()
        self.workers.append(new_worker)
        self.workers.remove(worker)
        logger.debug(self.workers)

    def _wait_for_workers(self):
        """Loop until any of the self.workers is alive.
        If a worker is dead and Kuyruk is running state, spawn a new worker.

        """
        start = time.time()
        any_alive = True
        while any_alive:
            any_alive = False
            for worker in list(self.workers):
                if worker.is_alive():
                    any_alive = True
                else:
                    if not self.stopping:
                        self._spawn_new_worker(worker)
                        any_alive = True

            if any_alive:
                logger.debug("Waiting for workers... "
                             "%i seconds passed" % (time.time() - start))
                time.sleep(1)

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigint(self, signum, frame):
        if self.stopping:
            logger.warning("Cold shutdown")
            self.kill_workers()
            sys.exit(1)
        else:
            logger.warning("Warm shutdown")
            self.stop_workers()

    def _handle_sigterm(self, signum, frame):
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
