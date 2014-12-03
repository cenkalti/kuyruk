from __future__ import absolute_import, print_function
import os
import sys
import json
import errno
import socket
import signal
import logging
import logging.config
import threading
import traceback
import multiprocessing
from time import time, sleep

from setproctitle import setproctitle

from kuyruk import importer, signals
from kuyruk.task import get_queue_name
from kuyruk.exceptions import Reject, Discard

logger = logging.getLogger(__name__)


class Worker(object):
    """Consumes tasks from a queue and runs them.

    :param app: An instance of :class:`~kuyruk.Kuyruk`
    :param args: Command line arguments

    """
    def __init__(self, app, args):
        self.kuyruk = app

        if not args.queue:
            raise ValueError("empty queue name")
        self.queue = get_queue_name(args.queue, local=args.local)

        self.shutdown_pending = threading.Event()
        self._pause = False
        self._consuming = False
        self._current_message = None
        if self.config.WORKER_MAX_LOAD is None:
            self.config.WORKER_MAX_LOAD = multiprocessing.cpu_count()

        signals.worker_init.send(self.kuyruk, worker=self)

    @property
    def config(self):
        return self.kuyruk.config

    def run(self):
        """Runs the worker and consumes messages from RabbitMQ.
        Returns only after `warm_shutdown()` is called.

        """
        setproctitle("kuyruk: worker on %s" % self.queue)

        self._setup_logging()

        signal.signal(signal.SIGINT, self._handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGQUIT, self._handle_sigquit)
        signal.signal(signal.SIGUSR1, print_stack)  # for debugging

        for f in (self._watch_load, self._shutdown_timer):
            t = threading.Thread(target=f)
            t.daemon = True
            t.start()

        self._consume_messages()

        signals.worker_shutdown.send(self.kuyruk, worker=self)
        logger.debug("End run worker")

    def _setup_logging(self):
        if self.config.WORKER_LOGGING_CONFIG:
            logging.config.fileConfig(self.config.WORKER_LOGGING_CONFIG)
        else:
            logging.getLogger('rabbitpy').level = logging.WARNING
            level = getattr(logging, self.config.WORKER_LOGGING_LEVEL.upper())
            fmt = "%(levelname).1s " \
                  "%(name)s.%(funcName)s:%(lineno)d - %(message)s"
            logging.basicConfig(level=level, format=fmt)

    def _consume_messages(self):
        with self.kuyruk as k:
            with k.channel() as ch:
                ch.queue_declare(
                    queue=self.queue, durable=True, auto_delete=False)

                # Set prefetch count to 1. If we don't set this, RabbitMQ keeps
                # sending messages while we are already working on a message.
                ch.basic_qos(0, 1, False)

                consumer_tag = '%s@%s' % (os.getpid(), socket.gethostname())
                while not self.shutdown_pending.is_set():
                    # Consume or pause
                    if self._pause and self._consuming:
                        ch.basic_cancel(consumer_tag)
                        logger.info('Consumer cancelled')
                        self._consuming = False
                    elif not self._pause and not self._consuming:
                        ch.basic_consume(queue=self.queue,
                                         consumer_tag=consumer_tag,
                                         callback=self._message_callback)
                        logger.info('Consumer started')
                        self._consuming = True

                    try:
                        ch.connection.drain_events(timeout=0.1)
                    except socket.error as e:
                        if isinstance(e, socket.timeout):
                            pass
                        elif e.errno == errno.EINTR:
                            pass  # happens when the process receives a signal
                        else:
                            raise
        logger.debug("End run worker")

    def _message_callback(self, message):
        # Save current message being processed so
        # we will be able to send ACK when we receive SIGQUIT."""
        self._current_message = message
        try:
            self._process_message(message)
        finally:
            self._current_message = None

    def _process_message(self, message):
        """Processes the message received from the queue."""
        try:
            description = json.loads(message.body)
        except Exception:
            message.channel.basic_reject(message.delivery_tag, requeue=False)
            logger.error("Cannot decode message. Dropping.")
        else:
            logger.info("Processing task: %r", description)
            self._process_description(message, description)

    def _process_description(self, message, description):
        try:
            task = importer.import_object(description['module'],
                                          description['function'])
            args, kwargs = description['args'], description['kwargs']
        except Exception:
            logger.error('Cannot import task')
            exc_info = sys.exc_info()
            signals.worker_failure.send(self.kuyruk, description=description,
                                        exc_info=exc_info, worker=self)
            message.channel.basic_reject(message.delivery_tag, requeue=False)
        else:
            self._process_task(message, description, task, args, kwargs)

    def _process_task(self, message, description, task, args, kwargs):
        try:
            task.apply(*args, **kwargs)
        except Reject:
            logger.warning('Task is rejected')
            sleep(1)  # Prevent cpu burning
            message.channel.basic_reject(message.delivery_tag, requeue=True)
        except Discard:
            logger.warning('Task is discarded')
            message.channel.basic_reject(message.delivery_tag, requeue=False)
        except Exception:
            logger.error('Task raised an exception')
            exc_info = sys.exc_info()
            logger.error(''.join(traceback.format_exception(*exc_info)))
            signals.worker_failure.send(self.kuyruk, description=description,
                                        task=task, args=args, kwargs=kwargs,
                                        exc_info=exc_info, worker=self)
            message.channel.basic_reject(message.delivery_tag, requeue=False)
        else:
            logger.info('Task is successful')
            message.channel.basic_ack(message.delivery_tag)
        finally:
            logger.debug("Task is processed")

    def _watch_load(self):
        """Pause consuming messages if lood goes above the allowed limit."""
        while not self.shutdown_pending.is_set():
            load = os.getloadavg()[0]
            if load > self.config.WORKER_MAX_LOAD:
                if self._pause is False:
                    logger.warning(
                        'Load is above the treshold (%.2f/%s), '
                        'pausing consumer', load, self.config.WORKER_MAX_LOAD)
                    self._pause = True
            else:
                if self._pause is True:
                    logger.warning(
                        'Load is below the treshold (%.2f/%s), '
                        'resuming consumer', load, self.config.WORKER_MAX_LOAD)
                    self._pause = False
            sleep(1)

    def _shutdown_timer(self):
        """Counts down from MAX_WORKER_RUN_TIME. When it reaches zero sutdown
        gracefully.

        """
        if not self.config.WORKER_MAX_RUN_TIME:
            return

        started = time()
        while not self.shutdown_pending.is_set():
            passed = time() - started
            remaining = self.config.WORKER_MAX_RUN_TIME - passed
            if remaining > 0:
                sleep(remaining)
            else:
                logger.warning('Run time reached zero')
                self.warm_shutdown()
                break

    def warm_shutdown(self):
        """Exits after the current task is finished."""
        logger.warning("Warm shutdown")
        self.shutdown_pending.set()

    def cold_shutdown(self):
        """Exits immediately."""
        logger.warning("Cold shutdown")
        _exit(0)

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
                self._current_message.channel.basic_reject(
                    self._current_message.delivery_tag, requeue=False)
                self._current_message.channel.close()
            except Exception:
                logger.critical("Cannot send ACK for the current task.")
                traceback.print_exc()
        logger.warning("Exiting...")
        _exit(0)


def _exit(code):
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


def print_stack(sig, frame):
    print('=' * 70)
    print(''.join(traceback.format_stack()))
    print('-' * 70)
