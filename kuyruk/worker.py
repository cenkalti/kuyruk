from __future__ import absolute_import
import os
import socket
import signal
import logging
import traceback
import multiprocessing
from time import sleep
from setproctitle import setproctitle
from kuyruk import importer
from kuyruk.queue import Queue
from kuyruk.channel import LazyChannel
from kuyruk.process import KuyrukProcess
from kuyruk.helpers import start_daemon_thread
from kuyruk.consumer import Consumer
from kuyruk.exceptions import Reject, ObjectNotFound, Timeout

try:
    import raven
except ImportError:
    raven = None

logger = logging.getLogger(__name__)


class Worker(KuyrukProcess):
    """Consumes messages from a queue and runs tasks.

    :param queue_name: The queue name to work on
    :param config: A :class:`~kuyurk.config.Config` object

    """
    def __init__(self, config, queue_name):
        super(Worker, self).__init__(config)
        self.channel = LazyChannel.from_config(config)
        is_local = queue_name.startswith('@')
        queue_name = queue_name.lstrip('@')
        self.queue_name = queue_name
        self.queue = Queue(queue_name, self.channel, local=is_local)
        self.consumer = Consumer(self.queue)
        self.working = False
        self.daemon_threads = [
            self.watch_master,
            self.watch_load,
            self.shutdown_timer,
        ]
        if self.config.MAX_LOAD is None:
            self.config.MAX_LOAD = multiprocessing.cpu_count()

        if self.config.SENTRY_DSN:
            if raven is None:
                raise ImportError('Cannot import raven. Please install it with '
                                  '"pip install raven".')
            self.sentry = raven.Client(self.config.SENTRY_DSN)
        else:
            self.sentry = None

    def run(self):
        """Runs the worker and opens a connection to RabbitMQ.
        After connection is opened, starts consuming messages.
        Consuming is cancelled if an external signal is received.

        """
        super(Worker, self).run()
        setproctitle("kuyruk: worker on %s" % self.queue_name)
        self.queue.declare()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.tx_select()
        self.import_modules()
        self.start_daemon_threads()
        self.maybe_start_manager_thread()
        self.consume_messages()
        logger.debug("End run worker")

    def consume_messages(self):
        """Consumes messages from the queue and run tasks until
        consumer is cancelled via a signal or another thread.

        """
        with self.consumer.consume() as messages:
            for message in messages:
                self.working = True
                self.process_message(message)
                self.channel.tx_commit()
                logger.debug("Committed transaction")
                self.working = False

    def start_daemon_threads(self):
        """Start the function as threads listed in self.daemon_thread."""
        for f in self.daemon_threads:
            start_daemon_thread(f)

    def import_modules(self):
        """Import modules defined in the configuration.
        This method is called before start consuming messages.

        """
        for module in self.config.IMPORTS:
            importer.import_module(module, self.config.IMPORT_PATH)

    def process_message(self, message):
        """Processes the message received from the queue."""
        task_description = message.get_object()
        try:
            task = self.import_task(task_description)
            args, kwargs = task_description['args'], task_description['kwargs']
            self.apply_task(task, args, kwargs)
        except Reject:
            logger.warning('Task is rejected')
            sleep(1)  # Prevent cpu burning
            message.reject()
        except ObjectNotFound:
            self.handle_not_found(message, task_description)
        except Timeout:
            self.handle_timeout(message, task_description)
        except Exception:
            self.handle_exception(message, task_description)
        else:
            logger.info('Task is successful')
            message.ack()

        logger.debug("Processing task is finished")

    def handle_exception(self, message, task_description):
        """Handles the exception while processing the message."""
        logger.error('Task raised an exception')
        logger.error(traceback.format_exc())
        retry_count = task_description.get('retry', 0)
        if retry_count:
            logger.debug('Retry count: %s', retry_count)
            message.discard()
            task_description['retry'] = retry_count - 1
            self.queue.send(task_description)
        else:
            logger.debug('No retry left')
            if self.sentry:
                ident = self.sentry.get_ident(self.sentry.captureException(
                    extra={'task_description': task_description}))
                logger.error("Exception caught; reference is %s", ident)

            message.discard()
            if self.config.SAVE_FAILED_TASKS:
                self.save_failed_task(task_description)

    def handle_not_found(self, message, task_description):
        """Called if the task is class task but the object with the given id
        is not found. The default action is logging the error and acking
        the message.

        """
        logger.error(
            "<%s.%s id=%r> is not found",
            task_description['module'],
            task_description['class'],
            task_description['args'][0])
        message.ack()

    def handle_timeout(self, message, task_description):
        """Called when the task is timed out while running the wrapped
        function.

        """
        logger.error('Task has timed out.')
        self.handle_exception(message, task_description)

    def save_failed_task(self, task_description):
        """Saves the task to ``kuyruk_failed`` queue. Failed tasks can be
        investigated later and requeued with ``kuyruk reuqueue`` command.

        """
        logger.info('Saving failed task')
        task_description['queue'] = self.queue_name
        task_description['hostname'] = socket.gethostname()
        task_description['exception'] = traceback.format_exc()
        failed_queue = Queue('kuyruk_failed', self.channel)
        failed_queue.send(task_description)
        logger.debug('Saved')

    def apply_task(self, task, args, kwargs):
        """Imports and runs the wrapped function in task."""

        # Fetch the object if class task
        if task.cls:
            if not isinstance(args[0], task.cls):
                obj = task.cls.get(args[0])
                if not obj:
                    raise ObjectNotFound
                args[0] = obj

        result = task.apply(args, kwargs)
        logger.debug('Result: %r', result)

    def import_task(self, task_description):
        """This is the method where user modules are loaded."""
        module, function, cls = (
            task_description['module'],
            task_description['function'],
            task_description['class'])
        return importer.import_task(
            module, cls, function, self.config.IMPORT_PATH)

    def is_master_alive(self):
        ppid = os.getppid()
        if ppid == 1:
            return False

        try:
            os.kill(ppid, 0)
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
        """Counts down from MAX_WORKER_RUN_TIME. When it reaches zero sutdown
        gracefully.

        """
        seconds = self.config.MAX_WORKER_RUN_TIME
        if seconds > 0:
            sleep(seconds)
            logger.warning('Run time reached zero, cancelling consume.')
            self.warm_shutdown()

    def register_signals(self):
        super(Worker, self).register_signals()
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        """Initiates a warm shutdown."""
        logger.warning("Catched SIGTERM")
        self.warm_shutdown()

    def warm_shutdown(self, sigint=False):
        """Shutdown gracefully."""
        super(Worker, self).warm_shutdown(sigint)
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
