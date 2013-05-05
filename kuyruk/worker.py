from __future__ import absolute_import
import os
import socket
import signal
import logging
import traceback
import threading
import multiprocessing
from time import time, sleep

from setproctitle import setproctitle

from kuyruk import importer
from kuyruk.queue import Queue
from kuyruk.channel import LazyChannel
from kuyruk.consumer import Consumer
from kuyruk.helpers import start_daemon_thread
from kuyruk.manager.client import ManagerClientThread

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
        self.shutdown_pending = threading.Event()

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
        self.channel.tx_select()

        # Start threads
        start_daemon_thread(self.watch_master)
        start_daemon_thread(self.watch_load)
        if self.config.MAX_RUN_TIME > 0:
            start_daemon_thread(self.shutdown_timer)
        self.started = time()
        ManagerClientThread(
            self.config.MANAGER_HOST,
            self.config.MANAGER_PORT,
            self, self.generate_message).start()

        # Consume messages
        with self.consumer.consume() as messages:
            for message in messages:
                self.process_task(message)
                self.channel.tx_commit()

        logger.debug("End run worker")

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
        task, cls = importer.import_task(
            module, cls, function, self.config.IMPORT_PATH)

        # Fetch object and prepent to args if class task
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

    def is_master_alive(self):
        try:
            os.kill(os.getppid(), 0)
            return True
        except OSError:
            return False

    def watch_master(self):
        """Watch the master and shutdown gracefully when it is dead."""
        while not self.shutdown_pending.is_set():
            if not self.is_master_alive():
                logger.critical('Master is dead')
                self.warm_shutdown()
            sleep(1)

    def watch_load(self):
        """Pause consuming messages if lood goes above the allowed limit."""
        while not self.shutdown_pending.is_set():
            load = os.getloadavg()[0]
            if load > self.config.MAX_LOAD:
                logger.warning('Load is high (%s), pausing consume', load)
                self.consumer.pause(10)
            sleep(1)

    def shutdown_timer(self):
        """Counts down from MAX_RUN_TIME. When it reaches zero sutdown
        gracefully.

        """
        sleep(self.config.MAX_RUN_TIME)
        logger.warning('Run time reached zero, cancelling consume.')
        self.warm_shutdown()

    def register_signals(self):
        # SIGINT is ignored because when pressed Ctrl-C
        # SIGINT sent to both master and workers while.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGUSR1, print_stack)  # for debugging

    def sigterm_handler(self, signum, frame):
        logger.warning("Catched SIGTERM")
        self.warm_shutdown()

    def warm_shutdown(self):
        """Shutdown gracefully."""
        logger.warning("Shutting down worker gracefully")
        self.shutdown_pending.set()
        self.consumer.stop()

    def generate_message(self):
        """Generate stats to be sent to manager."""
        method = self.queue.declare().method
        return {
            'type': 'worker',
            'hostname': socket.gethostname(),
            'uptime': self.uptime,
            'pid': os.getpid(),
            'ppid': os.getppid(),
            'queue': {
                'name': method.queue,
                'messages_ready': method.message_count,
                'consumers': method.consumer_count,
            }
        }

    @property
    def uptime(self):
        return int(time() - self.started)


def print_stack(sig, frame):
    print '=' * 70
    print ''.join(traceback.format_stack())
    print '-' * 70
