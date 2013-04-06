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

logger = logging.getLogger(__name__)


class Kuyruk(object):

    def __init__(self, config_module):
        DEFAULTS = {
            'KUYRUK_RABBIT_HOST': 'localhost',
            'KUYRUK_RABBIT_PORT': 5672,
            'KUYRUK_RABBIT_USER': 'guest',
            'KUYRUK_RABBIT_PASSWORD': 'guest',
            'KUYRUK_EAGER': False,
            'KUYRUK_MAX_RUN_TIME': None,
            'KUYRUK_MAX_TASKS': None,
            'KUYRUK_MAX_LOAD': None,
            'KUYRUK_WORKERS': {},
        }

        self.config = object()
        for k, v in DEFAULTS.iteritems():
            value = getattr(config_module, k, v)
            setattr(config_module, k[7:], value)

        self.config = config_module
        self.workers = []

    def task(self, queue='kuyruk'):
        """Wrap functions with this decorator to convert them
        to background tasks."""
        def decorator():
            def inner(f):
                queue_ = 'kuyruk' if callable(queue) else queue
                return Task(f, self, queue=queue_)
            return inner

        if callable(queue):
            logger.debug('task without args')
            return decorator()(queue)
        else:
            logger.debug('task with args')
            return decorator()

    def run(self, queues=None):
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
        logger.info('Starting to work on queues: %s', queues)

        for i, queue in enumerate(queues):
            worker = Worker(i, queue, self.config)
            worker.start()
            self.workers.append(worker)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.warning('Stopping kuyruk...')
            self.shutdown()

    def shutdown(self):
        try:
            self.warm_shutdown()
        except KeyboardInterrupt:
            self.cold_shutdown()

    def warm_shutdown(self):
        logger.warning('Warm shutdown')
        start = time.time()
        alive = True
        while alive:
            for worker in self.workers:
                if worker.is_alive():
                    logger.warning("%s is alive", worker.pid)
            else:
                alive = False

            logger.warning("Waiting for workers to finish their last task... "
                           "%i seconds passed" % (time.time() - start))
            time.sleep(1)

        sys.exit(0)

    def cold_shutdown(self):
        logger.critical('Cold shutdown!')
        logger.info('Sending SIGKILL to all workers...')
        for worker in self.workers:
            os.kill(worker.pid, signal.SIGKILL)

        sys.exit(1)


def parse_queues_str(s):
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
