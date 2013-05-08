from __future__ import absolute_import
import os
import socket
import signal
import logging
import traceback
from time import sleep
from setproctitle import setproctitle
from kuyruk import importer
from kuyruk.queue import Queue
from kuyruk.channel import LazyChannel
from kuyruk.process import KuyrukProcess
from kuyruk.helpers import start_daemon_thread
from kuyruk.consumer import Consumer

logger = logging.getLogger(__name__)


class Worker(KuyrukProcess):

    def __init__(self, queue_name, config):
        """
        :param queue_name: Queue name that this worker gets the messages from
        :param config: Configuration object
        """
        super(Worker, self).__init__(config)
        self.channel = LazyChannel.from_config(config)
        self.queue_name = queue_name
        is_local = queue_name.startswith('@')
        self.queue = Queue(queue_name, self.channel, local=is_local)
        self.consumer = Consumer(self.queue)
        self.working = False

    def run(self):
        """Run worker until stop flag is set.

        Since Worker class is derived from multiprocessing.Process,
        it will be invoked when worker.start() is called.

        """
        super(Worker, self).run()
        setproctitle("kuyruk: worker on %s" % self.queue_name)
        self.queue.declare()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.tx_select()
        self.import_modules()

        # Start threads
        start_daemon_thread(self.watch_master)
        start_daemon_thread(self.watch_load)
        self.maybe_start_manager_thread()
        if self.config.MAX_RUN_TIME > 0:
            start_daemon_thread(self.shutdown_timer)

        # Consume messages
        with self.consumer.consume() as messages:
            for message in messages:
                self.working = True
                self.process_task(message)
                self.channel.tx_commit()
                self.working = False

        logger.debug("End run worker")

    def import_modules(self):
        for module in self.config.IMPORTS:
            importer.import_task_module(module, self.config.IMPORT_PATH)

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
        result = task.apply(args, kwargs)
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
        # it is sent to both master and workers.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGUSR1, print_stack)  # for debugging

    def handle_sigterm(self, signum, frame):
        logger.warning("Catched SIGTERM")
        self.warm_shutdown()

    def warm_shutdown(self):
        """Shutdown gracefully."""
        logger.warning("Shutting down worker gracefully")
        self.shutdown_pending.set()
        self.consumer.stop()

    def get_stats(self):
        """Generate stats to be sent to manager."""
        method = self.queue.declare().method
        return {
            'type': 'worker',
            'hostname': socket.gethostname(),
            'uptime': self.uptime,
            'pid': os.getpid(),
            'ppid': os.getppid(),
            'working': self.working,
            'consuming': self.consumer.consuming,
            'queue': {
                'name': method.queue,
                'messages_ready': method.message_count,
                'consumers': method.consumer_count,
            }
        }


def print_stack(sig, frame):
    print '=' * 70
    print ''.join(traceback.format_stack())
    print '-' * 70
