from __future__ import absolute_import
import os
import sys
import socket
import signal
import logging
import logging.config
import threading
import traceback
import multiprocessing
from time import time, sleep
from functools import wraps
from contextlib import contextmanager

import rabbitpy
from setproctitle import setproctitle

import kuyruk
from kuyruk import importer
from kuyruk.helpers import start_daemon_thread, json_datetime, print_stack
from kuyruk.exceptions import Reject, ObjectNotFound, Timeout, InvalidTask

logger = logging.getLogger(__name__)


def set_current_task(f):
    """Save current task and it's arguments in self so we can send them to
    manager as stats."""
    @wraps(f)
    def inner(self, task, args, kwargs):
        self.current_task = task
        self.current_args = args
        self.current_kwargs = kwargs
        try:
            return f(self, task, args, kwargs)
        finally:
            self.current_task = None
            self.current_args = None
            self.current_kwargs = None
    return inner


class Worker(object):
    """Consumes messages from a queue and runs tasks.

    :param kuyruk: A :class:`~kuyurk.kuyruk.Kuyruk` instance
    :param queue_name: The queue name to work on

    """
    def __init__(self, kuyruk_, queue_name):
        assert isinstance(kuyruk_, kuyruk.Kuyruk)
        self.kuyruk = kuyruk_

        if not queue_name:
            raise ValueError("empty queue name")
        self.queue_name = queue_name

        is_local = queue_name.startswith('@')
        if is_local:
            self.queue_name = "%s.%s" % (queue_name[1:], socket.gethostname())

        self.queue = None
        self._pause = False
        self.shutdown_pending = threading.Event()
        self.current_message = None
        self.current_task = None
        self.current_args = None
        self.current_kwargs = None
        self.daemon_threads = [
            self.watch_load,
            self.shutdown_timer,
        ]
        if self.config.MAX_LOAD is None:
            self.config.MAX_LOAD = multiprocessing.cpu_count()

        if self.config.SENTRY_DSN:
            import raven
            self.sentry = raven.Client(self.config.SENTRY_DSN)
        else:
            self.sentry = None

    @property
    def config(self):
        return self.kuyruk.config

    def run(self):
        """Runs the worker and opens a connection to RabbitMQ.
        After connection is opened, starts consuming messages.
        Consuming is cancelled if an external signal is received.

        """
        self.setup_logging()
        signal.signal(signal.SIGINT, self.handle_sigint)
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGQUIT, self.handle_sigquit)
        signal.signal(signal.SIGUSR1, print_stack)  # for debugging

        signal.signal(signal.SIGUSR1, print_stack)  # for debugging
        setproctitle("kuyruk: worker on %s" % self.queue_name)
        self.import_modules()
        self.start_daemon_threads()
        self.consume_messages()
        logger.debug("End run worker")

    def setup_logging(self):
        if self.config.LOGGING_CONFIG:
            logging.config.fileConfig(self.config.LOGGING_CONFIG)
        else:
            logging.getLogger('rabbitpy').level = logging.WARNING
            level = getattr(logging, self.config.LOGGING_LEVEL.upper())
            fmt = "%(levelname).1s %(process)d " \
                  "%(name)s.%(funcName)s:%(lineno)d - %(message)s"
            logging.basicConfig(level=level, format=fmt)

    def handle_sigint(self, signum, frame):
        """If running from terminal pressing Ctrl-C will initiate a warm
        shutdown. The second interrupt will do a cold shutdown.

        """
        logger.warning("Handling SIGINT")
        if sys.stdin.isatty() and not self.shutdown_pending.is_set():
            self.warm_shutdown()
        else:
            self.cold_shutdown()
        logger.debug("Handled SIGINT")

    def handle_sigterm(self, signum, frame):
        """Initiates a warm shutdown."""
        logger.warning("Catched SIGTERM")
        self.warm_shutdown()

    def handle_sigquit(self, signum, frame):
        """Send ACK for the current task and exit."""
        logger.warning("Catched SIGQUIT")
        if self.current_message:
            try:
                logger.warning("Acking current task...")
                self.current_message.ack()
            except Exception:
                logger.critical("Cannot send ACK for the current task.")
                traceback.print_exc()
        logger.warning("Exiting...")
        sys.exit(0)

    def consume_messages(self):
        """Consumes messages from the queue and run tasks until
        consumer is cancelled via a signal or another thread.

        """
        with self.kuyruk.channel() as ch:
            logger.debug("Declaring queue")
            queue = rabbitpy.Queue(ch, name=self.queue_name, durable=True)
            queue.declare()
            logger.debug("Start consuming")
            with queue.consumer(prefetch=1) as consumer:
                self.queue = queue
                for message in consumer.next_message():
                    if message is None:
                        break
                    with self._set_current_message(message):
                        self.process_message(message)

    @contextmanager
    def _set_current_message(self, message):
        """Save current message being processed so we can send ack
        before exiting when SIGQUIT is received."""
        self.current_message = message
        try:
            yield message
        finally:
            self.current_message = None

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
        try:
            task_description = json_datetime.loads(message.body)
            logger.info("Processing task: %r", task_description)
        except Exception:
            message.ack()
            logger.error("Canot decode message. Dropped!")
            return

        try:
            task = self.import_task(task_description)
            task.message = message
            args, kwargs = task_description['args'], task_description['kwargs']
            self.apply_task(task, args, kwargs)
        except Reject:
            logger.warning('Task is rejected')
            sleep(1)  # Prevent cpu burning
            message.reject(requeue=True)
        except ObjectNotFound:
            self.handle_not_found(message, task_description)
        except Timeout:
            self.handle_timeout(message, task_description)
        except InvalidTask:
            self.handle_invalid(message, task_description)
        except Exception:
            self.handle_exception(message, task_description)
        else:
            logger.info('Task is successful')
            delattr(task, 'message')
            message.ack()
        finally:
            logger.debug("Task is processed")

    def handle_exception(self, message, task_description):
        """Handles the exception while processing the message."""
        logger.error('Task raised an exception')
        logger.error(traceback.format_exc())
        retry_count = task_description.get('retry', 0)
        if retry_count:
            logger.debug('Retry count: %s', retry_count)
            message.reject()
            task_description['retry'] = retry_count - 1
            body = json_datetime.dumps(task_description)
            msg = rabbitpy.Message(message.channel, body, properties={
                "delivery_mode": 2,
                "content_type": "application/json"})
            msg.publish("", self.queue_name, mandatory=True)
        else:
            logger.debug('No retry left')
            self.capture_exception(task_description)
            message.reject()

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

    def handle_invalid(self, message, task_description):
        """Called when the message from queue is invalid."""
        logger.error("Invalid message.")
        self.capture_exception(task_description)
        message.reject()

    @set_current_task
    def apply_task(self, task, args, kwargs):
        """Imports and runs the wrapped function in task."""
        result = task._run(*args, **kwargs)
        logger.debug('Result: %r', result)

    def import_task(self, task_description):
        """This is the method where user modules are loaded."""
        module, function, cls = (
            task_description['module'],
            task_description['function'],
            task_description['class'])
        return importer.import_task(
            module, cls, function, self.config.IMPORT_PATH)

    def capture_exception(self, task_description):
        """Sends the exceptin in current stack to Sentry."""
        if self.sentry:
            ident = self.sentry.get_ident(self.sentry.captureException(
                extra={
                    'task_description': task_description,
                    'hostname': socket.gethostname(),
                    'pid': os.getpid(),
                    'uptime': self.uptime}))
            logger.error("Exception caught; reference is %s", ident)
            task_description['sentry_id'] = ident

    def watch_load(self):
        """Pause consuming messages if lood goes above the allowed limit."""
        while not self.shutdown_pending.is_set():
            load = os.getloadavg()[0]
            if load > self.config.MAX_LOAD:
                logger.warning('Load is high (%s), pausing consume', load)
                self._pause = True
            else:
                self._pause = False
            sleep(1)

    def shutdown_timer(self):
        """Counts down from MAX_WORKER_RUN_TIME. When it reaches zero sutdown
        gracefully.

        """
        if not self.config.MAX_WORKER_RUN_TIME:
            return

        started = time()
        while True:
            passed = time() - started
            remaining = self.config.MAX_WORKER_RUN_TIME - passed
            if remaining > 0:
                sleep(remaining)
            else:
                logger.warning('Run time reached zero')
                self.warm_shutdown()
                break

    def warm_shutdown(self):
        """Exit after the last task is finished."""
        logger.warning("Warm shutdown")
        if self.queue is None:
            sys.exit(0)
        self.shutdown_pending.set()
        self.queue.stop_consuming()

    def cold_shutdown(self):
        """Exit immediately."""
        logger.warning("Cold shutdown")
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)
