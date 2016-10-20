from __future__ import absolute_import, print_function
import os
import sys
import json
import time
import errno
import socket
import signal
import logging
import logging.config
import warnings
import threading
import traceback
import multiprocessing

from kuyruk import importer, signals
from kuyruk.task import get_queue_name
from kuyruk.exceptions import Reject, Discard, ConnectionError

logger = logging.getLogger(__name__)


class Worker(object):
    """Consumes tasks from a queue and runs them.

    :param app: An instance of :class:`~kuyruk.Kuyruk`
    :param args: Command line arguments

    """
    def __init__(self, app, args):
        self.kuyruk = app

        if not args.queues:
            raise ValueError("no queue given")

        self.queues = [get_queue_name(q, local=args.local) for q in args.queues]
        self.shutdown_pending = threading.Event()
        self._pause = False
        self._started = None
        self.consuming = False
        self._current_message = None
        self.current_task = None
        self.current_args = None
        self.current_kwargs = None
        self._heartbeat_exc_info = None
        if self.config.WORKER_MAX_LOAD is None:
            self.config.WORKER_MAX_LOAD = multiprocessing.cpu_count()

        self._pid = os.getpid()
        self._hostname = socket.gethostname()

        signals.worker_init.send(self.kuyruk, worker=self)

    @property
    def queue(self):
        warnings.warn("Worker.queue is deprecated. Use Worker.queues instead.")
        return self.queues[0]

    @property
    def config(self):
        return self.kuyruk.config

    def run(self):
        """Runs the worker and consumes messages from RabbitMQ.
        Returns only after `shutdown()` is called.

        """
        # Lazy import setproctitle.
        # There is bug with the latest version of Python with
        # uWSGI and setproctitle combination.
        # Watch: https://github.com/unbit/uwsgi/issues/1030
        from setproctitle import setproctitle
        setproctitle("kuyruk: worker on %s" % ','.join(self.queues))

        self._setup_logging()

        signal.signal(signal.SIGINT, self._handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGHUP, self._handle_sighup)
        signal.signal(signal.SIGUSR1, self._handle_sigusr1)
        signal.signal(signal.SIGUSR2, self._handle_sigusr2)

        self._started = os.times()[4]

        for f in (self._watch_load, self._shutdown_timer):
            t = threading.Thread(target=f)
            t.daemon = True
            t.start()

        signals.worker_start.send(self.kuyruk, worker=self)
        self._consume_messages()
        signals.worker_shutdown.send(self.kuyruk, worker=self)

        logger.debug("End run worker")

    def _setup_logging(self):
        if self.config.WORKER_LOGGING_CONFIG:
            logging.config.fileConfig(self.config.WORKER_LOGGING_CONFIG)
        else:
            level = getattr(logging, self.config.WORKER_LOGGING_LEVEL.upper())
            fmt = "%(levelname).1s " \
                  "%(name)s.%(funcName)s:%(lineno)d - %(message)s"
            logging.basicConfig(level=level, format=fmt)

    def _consume_messages(self):
        with self.kuyruk.channel() as ch:
            # Set prefetch count to 1. If we don't set this, RabbitMQ keeps
            # sending messages while we are already working on a message.
            ch.basic_qos(0, 1, True)

            self._declare_queues(ch)
            while not self.shutdown_pending.is_set():
                # Consume or pause
                if self._pause and self.consuming:
                    self._cancel_queues(ch)
                    logger.info('Consumer cancelled')
                    self.consuming = False
                elif not self._pause and not self.consuming:
                    self._consume_queues(ch)
                    logger.info('Consumer started')
                    self.consuming = True

                if self._heartbeat_exc_info:
                    break

                try:
                    ch.connection.send_heartbeat()
                    ch.connection.drain_events(timeout=1)
                except socket.error as e:
                    if isinstance(e, socket.timeout):
                        pass
                    elif e.errno == errno.EINTR:
                        pass  # happens when the process receives a signal
                    else:
                        raise
        logger.debug("End run worker")

    def _consumer_tag(self, queue):
        return "%s:%s@%s" % (queue, self._pid, self._hostname)

    def _declare_queues(self, ch):
        for queue in self.queues:
            logger.debug("queue_declare: %s", queue)
            ch.queue_declare(
                queue=queue, durable=True, auto_delete=False)

    def _consume_queues(self, ch):
        for queue in self.queues:
            logger.debug("basic_consume: %s", queue)
            ch.basic_consume(queue=queue,
                             consumer_tag=self._consumer_tag(queue),
                             callback=self._process_message)

    def _cancel_queues(self, ch):
        for queue in self.queues:
            logger.debug("basic_cancel: %s", queue)
            ch.basic_cancel(self._consumer_tag(queue))

    def _process_message(self, message):
        """Processes the message received from the queue."""
        if self.shutdown_pending.is_set():
            return

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
            self._current_message = message
            self.current_task = task
            self.current_args = args
            self.current_kwargs = kwargs
            stop_heartbeat = threading.Event()
            heartbeat_thread = threading.Thread(
                target=self._heartbeat_tick,
                args=(message.channel.connection, stop_heartbeat))
            heartbeat_thread.start()
            try:
                self._apply_task(task, args, kwargs)
            finally:
                self._current_message = None
                self.current_task = None
                self.current_args = None
                self.current_kwargs = None
                stop_heartbeat.set()
                heartbeat_thread.join()
        except Reject:
            logger.warning('Task is rejected')
            time.sleep(1)  # Prevent cpu burning
            message.channel.basic_reject(message.delivery_tag, requeue=True)
        except Discard:
            logger.warning('Task is discarded')
            message.channel.basic_reject(message.delivery_tag, requeue=False)
        except ConnectionError:
            pass
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

    @staticmethod
    def _apply_task(task, args, kwargs):
        """Logs the time spent while running the task."""
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        start = time.time()
        try:
            task.apply(*args, **kwargs)
        finally:
            end = time.time()
            logger.info("%s finished in %i seconds." % (task.name, end - start))

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
            time.sleep(1)

    @property
    def uptime(self):
        if self._started is not None:
            return os.times()[4] - self._started

    def _shutdown_timer(self):
        """Counts down from MAX_WORKER_RUN_TIME. When it reaches zero sutdown
        gracefully.

        """
        if not self.config.WORKER_MAX_RUN_TIME:
            return

        while not self.shutdown_pending.is_set():
            remaining = self.config.WORKER_MAX_RUN_TIME - self.uptime
            if remaining > 0:
                time.sleep(remaining)
            else:
                logger.warning('Run time reached zero')
                self.shutdown()
                break

    def shutdown(self):
        """Exits after the current task is finished."""
        logger.warning("Shutdown requested")
        self.shutdown_pending.set()

    def _handle_sigint(self, signum, frame):
        """Shutdown after processing current task."""
        logger.warning("Catched SIGINT")
        self.shutdown()

    def _handle_sigterm(self, signum, frame):
        """Shutdown after processing current task."""
        logger.warning("Catched SIGTERM")
        self.shutdown()

    def _handle_sighup(self, signum, frame):
        """Used internally to fail the task when connection to RabbitMQ is
        lost during the execution of the task.

        """
        logger.warning("Catched SIGHUP")
        raise ConnectionError(self._heartbeat_exc_info)

    @staticmethod
    def _handle_sigusr1(signum, frame):
        """Print stacktrace."""
        print('=' * 70)
        print(''.join(traceback.format_stack()))
        print('-' * 70)

    def _handle_sigusr2(self, signum, frame):
        """Drop current task."""
        logger.warning("Catched SIGUSR2")
        if self._current_message:
            logger.warning("Dropping current task...")
            raise Discard

    def drop_task(self):
        os.kill(os.getpid(), signal.SIGUSR2)

    def _heartbeat_tick(self, connection, stop_event):
        while not stop_event.wait(1):
            try:
                connection.send_heartbeat()
            except socket.timeout:
                pass
            except Exception as e:
                logger.error(e)
                self._heartbeat_exc_info = sys.exc_info()
                os.kill(os.getpid(), signal.SIGHUP)
                break
