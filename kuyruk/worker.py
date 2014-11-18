from __future__ import absolute_import
import os
import sys
import json
import socket
import signal
import logging
import logging.config
import threading
import traceback
import multiprocessing
from time import time, sleep
from contextlib import contextmanager

from setproctitle import setproctitle

import kuyruk
from kuyruk import importer, Config
from kuyruk.task import get_queue_name
from kuyruk.exceptions import Reject, Discard, ObjectNotFound, InvalidTask

logger = logging.getLogger(__name__)


class Worker(object):
    """Consumes tasks from a queue and runs them.

    :param config: A :class:`~kuyruk.config.Config` instance
    :param queue: The queue name to work on

    """
    def __init__(self, config, queue):
        assert isinstance(config, Config)
        self.config = config

        is_local = queue.startswith('@')
        queue = queue.lstrip('@')
        if not queue:
            raise ValueError("empty queue name")

        self.queue = get_queue_name(queue, local=is_local)
        self._consumer_tag = '%s@%s' % (os.getpid(), socket.gethostname())
        self._channel = None
        self.shutdown_pending = threading.Event()
        self._pause = False
        self._consuming = False
        self._current_message = None
        self._current_task = None
        self._current_args = None
        self._current_kwargs = None
        self._daemon_threads = [
            self._watch_load,
            self._shutdown_timer,
        ]
        if self.config.MAX_LOAD is None:
            self.config.MAX_LOAD = multiprocessing.cpu_count()

    def run(self):
        """Runs the worker and opens a connection to RabbitMQ.
        After connection is opened, starts consuming messages.
        Consuming is cancelled if an external signal is received.

        """
        setproctitle("kuyruk: worker on %s" % self.queue)
        self._setup_logging()
        signal.signal(signal.SIGINT, self._handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGQUIT, self._handle_sigquit)
        signal.signal(signal.SIGUSR1, print_stack)  # for debugging

        self._start_daemon_threads()
        self._consume_messages()
        logger.debug("End run worker")

    def _consume_messages(self):
        k = kuyruk.Kuyruk(self.config)
        with k.channel() as ch:
            self._channel = ch
            ch.queue_declare(queue=self.queue, durable=True, auto_delete=False)
            # Set prefetch count to 1. If we don't set this, RabbitMQ keeps
            # sending messages while we are already working on a message.
            ch.basic_qos(0, 1, False)
            while not self.shutdown_pending.is_set():
                if self._pause and self._consuming:
                    ch.basic_cancel(self._consumer_tag)
                    logger.info('Consumer cancelled')
                    self._consuming = False
                elif not self._pause and not self._consuming:
                    ch.basic_consume(queue=self.queue,
                                     consumer_tag=self._consumer_tag,
                                     callback=self._message_callback)
                    logger.info('Consumer started')
                    self._consuming = True

                try:
                    ch.connection.drain_events(timeout=0.1)
                except socket.error as e:
                    if isinstance(e, socket.timeout):
                        pass
                    elif e.errno == socket.EINTR:
                        pass
                    else:
                        raise

        k.close()
        logger.debug("End run worker")

    def _setup_logging(self):
        if self.config.LOGGING_CONFIG:
            logging.config.fileConfig(self.config.LOGGING_CONFIG)
        else:
            logging.getLogger('rabbitpy').level = logging.WARNING
            level = getattr(logging, self.config.LOGGING_LEVEL.upper())
            fmt = "%(levelname).1s %(process)d " \
                  "%(name)s.%(funcName)s:%(lineno)d - %(message)s"
            logging.basicConfig(level=level, format=fmt)

    def _handle_sigint(self, signum, frame):
        """If running from terminal pressing Ctrl-C will initiate a warm
        shutdown. The second interrupt will do a cold shutdown.

        """
        logger.warning("Handling SIGINT")
        if sys.stdin.isatty() and not self.shutdown_pending.is_set():
            self.warm_shutdown()
        else:
            self.cold_shutdown()
        logger.debug("Handled SIGINT")

    def _handle_sigterm(self, signum, frame):
        """Initiates a warm shutdown."""
        logger.warning("Catched SIGTERM")
        self.warm_shutdown()

    def _handle_sigquit(self, signum, frame):
        """Drop the current task and exit."""
        logger.warning("Catched SIGQUIT")
        if self._current_message:
            try:
                logger.warning("Dropping current task...")
                self._channel.basic_reject(self._current_message.delivery_tag,
                                          requeue=False)
                self._channel.close()
            except Exception:
                logger.critical("Cannot send ACK for the current task.")
                traceback.print_exc()
        logger.warning("Exiting...")
        _exit(0)

    def _message_callback(self, message):
        """Consumes messages from the queue and run tasks until
        consumer is cancelled via a signal or another thread.

        """
        with self._set_current_message(message):
            self._process_message(message)

    @contextmanager
    def _set_current_message(self, message):
        """Save current message being processed so we can send ack
        before exiting when SIGQUIT is received."""
        self._current_message = message
        try:
            yield
        finally:
            self._current_message = None

    @contextmanager
    def _set_current_task(self, task, args, kwargs):
        """Save current message being processed so we can send ack
        before exiting when SIGQUIT is received."""
        self._current_task = task
        self._current_args = args
        self._current_kwargs = kwargs
        try:
            yield
        finally:
            self._current_task = None
            self._current_args = None
            self._current_kwargs = None

    def _start_daemon_threads(self):
        """Start the function as threads listed in self.daemon_thread."""
        for f in self._daemon_threads:
            start_daemon_thread(f)

    def _process_message(self, message):
        """Processes the message received from the queue."""
        try:
            task_description = json.loads(message.body)
        except Exception:
            self._channel.basic_reject(message.delivery_tag, requeue=False)
            logger.error("Canot decode message. Dropped the message!")
        else:
            logger.info("Processing task: %r", task_description)
            self._process_task_description(message, task_description)

    def _process_task_description(self, message, task_description):
        try:
            task = importer.import_task(task_description['module'],
                                        task_description['class'],
                                        task_description['function'])
        except Exception:
            logger.error('Cannot import task')
            self._channel.basic_reject(message.delivery_tag, requeue=False)
        else:
            self._process_task(message, task_description, task)

    def _process_task(self, message, task_description, task):
        try:
            args, kwargs = task_description['args'], task_description['kwargs']
            with self._set_current_task(task, args, kwargs):
                self.apply_task(task, args, kwargs)
        except Reject:
            logger.warning('Task is rejected')
            if os.environ['KUYRUK_TESTING'] != 'True':
                sleep(1)  # Prevent cpu burning
            self._channel.basic_reject(message.delivery_tag, requeue=True)
        except Discard:
            logger.warning('Task is discarded')
            self._channel.basic_reject(message.delivery_tag, requeue=False)
        except ObjectNotFound:
            logger.warning('Object not found')
            self._handle_not_found(message, task_description)
        except InvalidTask:
            logger.error('Invalid task')
            self._handle_invalid(message, task_description)
        except Exception:
            logger.error('Task raised an exception')
            self._handle_exception(sys.exc_info(), message)
        else:
            logger.info('Task is successful')
            self._channel.basic_ack(message.delivery_tag)
        finally:
            logger.debug("Task is processed")

    def apply_task(self, task, args, kwargs):
        """Runs the wrapped function in task."""
        task._run(*args, **kwargs)

    def _handle_exception(self, exc_info, message):
        """Handles the exception while processing the message."""
        logger.error(traceback.format_exception(*exc_info))
        self._channel.basic_reject(message.delivery_tag, requeue=False)

    def _handle_not_found(self, message, task_description):
        """Called if the task is class task but the object with the given id
        is not found. The default action is logging the error and dropping
        the message.

        """
        logger.warning(
            "<%s.%s id=%r> is not found",
            task_description['module'],
            task_description['class'],
            task_description['args'][0])
        self._channel.basic_reject(message.delivery_tag, requeue=False)

    def _handle_invalid(self, message, task_description):
        """Called when the task is invalid."""
        self._channel.basic_reject(message.delivery_tag, requeue=False)

    def _watch_load(self):
        """Pause consuming messages if lood goes above the allowed limit."""
        while not self.shutdown_pending.is_set():
            load = os.getloadavg()[0]
            if load > self.config.MAX_LOAD:
                logger.warning('Load is high (%s), pausing consumer', load)
                self._pause = True
            else:
                self._pause = False
            sleep(1)

    def _shutdown_timer(self):
        """Counts down from MAX_WORKER_RUN_TIME. When it reaches zero sutdown
        gracefully.

        """
        if not self.config.MAX_WORKER_RUN_TIME:
            return

        started = time()
        while not self.shutdown_pending.is_set():
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
        self.shutdown_pending.set()

    def cold_shutdown(self):
        """Exit immediately."""
        logger.warning("Cold shutdown")
        _exit(0)


def _exit(code):
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


def start_daemon_thread(target, args=()):
    t = threading.Thread(target=target, args=args)
    t.daemon = True
    t.start()
    return t


def print_stack(sig, frame):
    print '=' * 70
    print ''.join(traceback.format_stack())
    print '-' * 70
