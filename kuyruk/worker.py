import os
import time
import signal
import logging
import traceback
import multiprocessing

from setproctitle import setproctitle

from . import loader
from .queue import Queue
from .exceptions import Reject
from .connection import LazyConnection

logger = logging.getLogger(__name__)


class Worker(multiprocessing.Process):

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
        setproctitle('kuyruk: worker')
        self.started = time.time()
        self.channel.tx_select()
        while self._runnable():
            if self._max_load():
                logger.debug('Load is over %s. Sleeping 10 seconds...')
                time.sleep(10)
                continue

            message = self.queue.receive()
            if message is None:
                logger.debug('No task. Sleeping 1 second...')
                time.sleep(1)
                continue

            self.work(message)
            self.channel.tx_commit()
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
        # sleep() calls below prevent cpu burning
        except Reject:
            logger.info('Task is rejected')
            time.sleep(1)
            self.queue.reject(tag)
        except Exception:
            logger.error('Task raised an exception')
            logger.error(traceback.format_exc())
            time.sleep(1)
            self.handle_exception(tag, task_description)

    def handle_exception(self, tag, task_description):
        retry_count = task_description.get('retry', 0)
        if retry_count:
            logger.debug('Retry count: %s', retry_count)
            self.queue.discard(tag)
            task_description['retry'] = retry_count - 1
            self.queue.send(task_description)
        else:
            logger.debug('No retry left')
            self.queue.discard(tag)
            if self.config.SAVE_FAILED_TASKS:
                self.save_failed_task(task_description)

    def save_failed_task(self, task_description):
        logger.info('Saving failed task')
        failed_queue = Queue('kuyruk_failed', self.channel)
        failed_queue.send(task_description)

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
