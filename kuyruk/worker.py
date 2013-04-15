import os
import time
import pickle
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

    def __init__(self, queue_name, config, master_pid):
        """
        :param queue_name: Qeueu name that this worker gets the messages from
        :param config: Configuration object
        """
        super(Worker, self).__init__()
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

    def run(self):
        """Run worker until stop flag is set.

        Since Worker class is derived from multiprocessing.Process,
        it will be invoked when worker.start() is called.

        """
        self._register_signals()
        setproctitle('kuyruk: worker')
        self.started = time.time()
        self.queue.declare()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.tx_select()

        logger.info('Starting consume')
        for message in self.channel.consume(self.queue_name):
            self.on_message(None, *message)
            if not self._runnable():
                break

        #     if self._max_load():
        #         logger.debug('Load is over %s. Sleeping 10 seconds...')
        #         time.sleep(10)
        #         continue

        logger.debug("End run worker")

    def on_message(self, channel, method, properties, body):
        obj = pickle.loads(body)
        logger.debug(
            'Message received in queue: %s message: %s', self.name, obj)
        self.work(method.delivery_tag, obj)
        self.channel.tx_commit()

    def stop(self):
        """Set stop flag.

        Worker will be stopped after current task is processed. If the task is
        stuck (i.e. in infinite loop) it may never be stopped.

        """
        logger.warning("Stopping %s...", self)
        self._stop.set()
        self.channel.cancel()

    def work(self, tag, task_description):
        logger.info('got message: %s', task_description)

        try:
            self.process_task(task_description)
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
        else:
            logger.debug('Task is successful')
            self.queue.ack(tag)

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
        task_description['queue'] = self.queue_name
        failed_queue = Queue('kuyruk_failed', self.channel)
        failed_queue.send(task_description)
        logger.debug('Saved')

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

    def _max_load(self):
        return os.getloadavg()[0] > self.config.MAX_LOAD

    def _register_signals(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self._sitterm_handler)

    def _sitterm_handler(self, signum, frame):
        logger.warning("Catched SIGTERM")
        self.stop()
