import os
import signal
import logging
import traceback
import threading
import multiprocessing
from time import sleep
from functools import wraps

from setproctitle import setproctitle

from . import loader
from .queue import Queue
from .channel import LazyChannel
from .consumer import Consumer

logger = logging.getLogger(__name__)


class Worker(multiprocessing.Process):

    def __init__(self, queue_name, config):
        """
        :param queue_name: Queue name that this worker gets the messages from
        :param config: Configuration object
        """
        super(Worker, self).__init__()
        self.config = config
        self.channel = LazyChannel(
            self.config.RABBIT_HOST, self.config.RABBIT_PORT,
            self.config.RABBIT_USER, self.config.RABBIT_PASSWORD)
        self.queue_name = queue_name
        is_local = queue_name.startswith('@')
        self.queue = Queue(queue_name, self.channel, local=is_local)
        self.consumer = Consumer(self.queue)
        self.processor = None

    def run(self):
        """Run worker until stop flag is set.

        Since Worker class is derived from multiprocessing.Process,
        it will be invoked when worker.start() is called.

        """
        logger.debug('Process id: %s', os.getpid())
        logger.debug('Process group id: %s', os.getpgrp())
        setproctitle("kuyruk: worker on %s" % self.queue_name)
        self.register_signals()
        self.queue.declare()
        self.channel.basic_qos(prefetch_count=1)
        # self.channel.tx_select()

        start_thread(self.watch_master, daemon=True)
        if self.config.MAX_RUN_TIME > 0:
            start_thread(self.count_run_time, daemon=True)

        logger.info('Starting consume')
        for message in self.consumer:
            self.on_task(message)

        if self.processor:
            self.processor.join()

        logger.debug("End run worker")

    def on_task(self, message):
        logger.info('Task received: %s', message)
        if self.is_load_high():
            logger.warning('Load is high, rejecting task')
            message.reject()
            # self.channel.tx_commit()
            self.consumer.pause(30)
        else:
            target = self.stop_consumer_on_exception(self.process_task)
            self.processor = start_thread(target, (message, ))
            # self.channel.tx_commit()

    def stop_consumer_on_exception(self, f):
        @wraps(f)
        def inner(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception:
                traceback.print_exc()
                self.consumer.stop()
        return inner

    def process_task(self, message):
        from kuyruk import Kuyruk

        task_description = message.get_object()
        try:
            self.import_and_call_task(task_description)
        # sleep() calls below prevent cpu burning
        except Kuyruk.Reject:
            logger.warning('Task is rejected')
            sleep(1)
            message.reject()
        except Exception:
            logger.error('Task raised an exception')
            logger.error(traceback.format_exc())
            sleep(1)
            self.handle_exception(message, task_description)
        else:
            logger.info('Task is successful')
            message.ack()
        logger.debug("Processing task is finished")

    def handle_exception(self, message, task_description):
        retry_count = task_description.get('retry', 0)
        if retry_count:
            logger.debug('Retry count: %s', retry_count)
            message.discard()
            task_description['retry'] = retry_count - 1
            self.queue.send(task_description)
        else:
            logger.debug('No retry left')
            message.discard()
            if self.config.SAVE_FAILED_TASKS:
                self.save_failed_task(task_description)

    def save_failed_task(self, task_description):
        logger.info('Saving failed task')
        task_description['queue'] = self.queue_name
        failed_queue = Queue('kuyruk_failed', self.channel)
        failed_queue.send(task_description)
        logger.debug('Saved')

    def import_and_call_task(self, task_description):
        """Call task function.
        This is the method where user modules are loaded.

        """
        module, function, cls, object_id, args, kwargs = (
            task_description['module'],
            task_description['function'],
            task_description['class'],
            task_description['object_id'],
            task_description['args'],
            task_description['kwargs'])
        task, cls = loader.import_task(module, cls, function)

        if cls:
            obj = cls.get(object_id)
            if not obj:
                logger.warning("<%s.%s id=%r> is not found",
                               module, cls.__name__, object_id)
                return

            args = list(args)
            args.insert(0, obj)

        logger.debug('Task %r will be executed with args=%s and kwargs=%s',
                     task, args, kwargs)
        result = task.run(args, kwargs)
        logger.debug('Result: %r', result)

    def is_master_dead(self):
        try:
            os.kill(os.getppid(), 0)
        except OSError:
            return True

    def watch_master(self):
        """Watch the master and send itself SIGTERM when it is dead."""
        while True:
            if self.is_master_dead():
                logger.critical('Master is dead')
                self.consumer.stop()
                break
            else:
                sleep(1)

    def count_run_time(self):
        """Counts down from MAX_RUN_TIME. When it reaches
        zero it sends a signal to itself for graceful shutdown.

        """
        sleep(self.config.MAX_RUN_TIME)
        logger.critical('Run time reached zero, cancelling consume.')
        self.consumer.stop()

    def is_load_high(self):
        return os.getloadavg()[0] > self.config.MAX_LOAD

    def register_signals(self):
        # SIGINT is ignored because when pressed Ctrl-C
        # SIGINT sent to both master and workers while.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGUSR1, print_stack)  # for debugging

    def sigterm_handler(self, signum, frame):
        logger.warning("Catched SIGTERM")
        logger.warning("Stopping %s...", self)
        self.consumer.stop()


def start_thread(target, args=(), daemon=False):
    t = threading.Thread(target=target, args=args)
    t.daemon = daemon
    t.start()
    return t


def print_stack(sig, frame):
    print '=' * 70
    print ''.join(traceback.format_stack())
    print '-' * 70
