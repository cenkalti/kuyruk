import os
import sys
import time
import socket
import signal
import logging
import itertools
import multiprocessing

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

    STATE_INIT = 0
    STATE_STARTING = 1
    STATE_STARTED = 2
    STATE_STOPPING = 3
    STATE_STOPPED = 4

    def __init__(self, config_object={}):
        """
        :param config_object: See config.py for default values.

        """
        self.config = Config(config_object)
        self.workers = []
        self.last_worker_number = 0
        self.state = self.STATE_INIT

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
        self.state = self.STATE_STARTING
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
        self.state = self.STATE_STARTED
        self._wait_for_workers()
        self.state = self.STATE_STOPPED

    def stop(self):
        """Stop all running workers. Wait until their job is finished.
        If called second time, kill workers immediately.

        """
        logger.debug('Current state: %s', self.state)
        if self.state == self.STATE_STARTED:
            logger.warning("Warm shutdown")
            self.state = self.STATE_STOPPING
            for worker in self.workers:
                worker.stop()
        elif self.state == self.STATE_STOPPING:
            logger.warning("Cold shutdown")
            for worker in self.workers:
                os.kill(worker.pid, signal.SIGKILL)
            sys.exit(1)

    def _start_workers(self, queues):
        """Start a new worker for each queue name"""
        logger.info('Starting to work on queues: %s', queues)
        for i, queue in enumerate(queues):
            worker = Worker(i, queue, self.config)
            worker.start()
            self.workers.append(worker)

    def _spawn_new_worker(self, worker):
        """Spawn a new process with parameters same as the old worker."""
        logger.debug("Spawning new worker")
        num = self._get_next_worker_number()
        new_worker = Worker(num, worker.queue_name, self.config)
        self.workers.append(new_worker)
        self.workers.remove(worker)

    def _wait_for_workers(self):
        """Loop until any of the self.workers is alive.
        If a worker is dead and Kuyruk is running state, spawn a new worker.

        """
        start = time.time()
        alive = True  # is any worker alive?
        while alive:
            for worker in list(self.workers):
                if worker.is_alive():
                    alive = True
                    break
                else:
                    if self.state == self.STATE_STARTED:
                        self._spawn_new_worker(worker)
                        alive = True
                        break
            else:
                alive = False

            if alive:
                logger.debug("Waiting for workers... "
                             "%i seconds passed" % (time.time() - start))
                time.sleep(1)

    def _get_next_worker_number(self):
        n = self.last_worker_number
        self.last_worker_number += 1
        return n

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self.stop()


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
