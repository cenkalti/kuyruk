import os
import time
import signal
import logging
import traceback
import multiprocessing

from . import loader
from .queue import Queue
from .exceptions import Reject
from .connection import LazyConnection

logger = logging.getLogger(__name__)


class Worker(multiprocessing.Process):

    RESULT_OK = 0
    RESULT_ERROR = 1
    RESULT_REJECT = 2

    def __init__(self, number, queue_name, config):
        super(Worker, self).__init__(name="Worker-%i" % number)
        self.config = config
        self.connection = LazyConnection(
            self.config.RABBIT_HOST, self.config.RABBIT_PORT,
            self.config.RABBIT_USER, self.config.RABBIT_PASSWORD)
        self.channel = self.connection.channel()
        self.queue = Queue(
            queue_name, self.channel, local=queue_name.startswith('@'))
        self._stop = multiprocessing.Event()
        self.num_tasks = 0

    def run(self):
        self._register_signals()
        self.started = time.time()
        while self._runnable():
            if self._max_load():
                logger.debug('Load is over %s. Sleeping 10 seconds...')
                time.sleep(10)
                continue

            message = self.queue.receive()
            if message is None:
                logger.debug('No tasks. Sleeping 1 second...')
                time.sleep(1)
                continue

            self.work(message)
            self.num_tasks += 1

    def work(self, message):
        tag, task_description = message
        logger.info('got message: %s', task_description)

        try:
            self.process_task(task_description)
            logger.debug('Task is successful')
            self.queue.ack(tag)
        except Reject:
            logger.info('Task is rejected')
            self.queue.reject(tag)
        except Exception:
            logger.error('Task raised an exception')
            print '*' * 80
            traceback.print_exc()
            self.queue.discard(tag)

    def process_task(self, task_description):
        fname, args, kwargs = (task_description['f'],
                               task_description['args'],
                               task_description['kwargs'])
        task = loader.import_task(fname)
        logger.debug(
            'Task %r will be executed with args=%r and kwargs=%r',
            task, args, kwargs)

        result = task.f(*args, **kwargs)
        logger.debug('Result: %r', result)

    def _max_run_time(self):
        if self.config.MAX_RUN_TIME is not None:
            passed_seconds = time.time() - self.started
            if passed_seconds >= self.config.MAX_RUN_TIME:
                logger.warning('Kuyruk run for %s seconds', passed_seconds)
                self.stop()

    def _max_tasks(self):
        if self.num_tasks == self.config.MAX_TASKS:
            logger.warning('Kuyruk has processed %s tasks',
                           self.config.MAX_TASKS)
            self.stop()

    def _max_load(self):
        return os.getloadavg()[0] > self.config.MAX_LOAD

    def _runnable(self):
        self._max_run_time()
        self._max_tasks()
        return not self._stop.is_set()

    def stop(self):
        logger.warning("Stopping %s...", self)
        self._stop.set()

    def _register_signals(self):
        def handler(signum, frame):
            logger.warning("Catched %s" % signum)
            self.stop()
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)
