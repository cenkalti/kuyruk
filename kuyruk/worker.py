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

    def __init__(self, number, queue_name, config, master_pid):
        """
        :param number: Worker number. This is used for displaying purposes.
        :param queue_name: Qeueu name that this worker gets the messages from
        :param config: Configuration object
        """
        super(Worker, self).__init__(name="Worker-%i" % number)
        self.config = config
        self.master_pid = master_pid
        self.connection = LazyConnection(
            self.config.RABBIT_HOST, self.config.RABBIT_PORT,
            self.config.RABBIT_USER, self.config.RABBIT_PASSWORD)
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        is_local = queue_name.startswith('@')
        self.queue = Queue(queue_name, self.channel, local=is_local)
        self._stop = multiprocessing.Event()
        self.num_tasks = 0

    def run(self):
        """Run worker until stop flag is set.

        Since Worker class is derived from multiprocessing.Process,
        it will be invoked when worker.start() is called.

        """
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
        logger.debug("End run")

    def stop(self):
        """Set stop flag.

        Worker will be stopped after current task is processed. If the task is
        stuck (i.e. in infinite loop) it may never be stopped.

        """
        logger.warning("Stopping %s...", self)
        self._stop.set()

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
            time.sleep(1)
        except Exception:
            logger.error('Task raised an exception')
            print '*' * 80
            print traceback.format_exc()
            if self.config.SAVE_FAILED_TASKS:
                raise NotImplementedError
                self.queue.discard(tag)
            else:
                time.sleep(1)
                self.queue.recover()

    def process_task(self, task_description):
        """Call task function.
        This is the method where user modules are loaded.

        """
        fname, args, kwargs = (task_description['f'],
                               task_description['args'],
                               task_description['kwargs'])
        task = loader.import_task(fname)
        logger.debug(
            'Task %r will be executed with args=%r and kwargs=%r',
            task, args, kwargs)

        result = task.f(*args, **kwargs)
        logger.debug('Result: %r', result)

    def _runnable(self):
        self._check_master()
        self._max_run_time()
        self._max_tasks()
        return not self._stop.is_set()

    def _check_master(self):
        try:
            os.kill(self.master_pid, 0)
        except OSError:
            self.stop()

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

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.warning("Catched %s" % signum)
        self.stop()
