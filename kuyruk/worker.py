import os
import sys
import json
import platform
import socket
import signal
import logging
import logging.config
import threading
import traceback
import argparse
import multiprocessing
from time import monotonic
from typing import Dict, Any, List, Tuple, Type, Optional  # noqa

import amqp

from kuyruk import importer, signals
from kuyruk.kuyruk import Kuyruk
from kuyruk.task import Task
from kuyruk.heartbeat import Heartbeat
from kuyruk.exceptions import Reject, Discard, HeartbeatError, ExcInfoType

logger = logging.getLogger(__name__)


class Worker:
    """Consumes tasks from queues and runs them.

    :param app: An instance of :class:`~kuyruk.Kuyruk`
    :param args: Command line arguments

    """
    def __init__(self, app: Kuyruk, args: argparse.Namespace) -> None:
        self.kuyruk = app

        if not args.queues:
            args.queues = ['kuyruk']

        def add_host(queue: str) -> str:
            if queue.endswith('.localhost'):
                queue = queue.rsplit('.localhost')[0]
                return "%s.%s" % (queue, self._hostname)
            else:
                return queue

        self._hostname = socket.gethostname()
        self.queues = [add_host(q) for q in args.queues]
        self._tasks = {}  # type: Dict[Tuple[str, str], Task]
        self.shutdown_pending = threading.Event()
        self.consuming = False
        self.current_task = None  # type: Task
        self.current_args = None  # type: Tuple
        self.current_kwargs = None  # type: Dict[str, Any]

        self._started_at = None  # type: float
        self._pid = os.getpid()

        self._logging_level = app.config.WORKER_LOGGING_LEVEL
        if args.logging_level is not None:
            self._logging_level = args.logging_level

        self._max_run_time = app.config.WORKER_MAX_RUN_TIME
        if args.max_run_time is not None:
            self._max_run_time = args.max_run_time

        self._max_load = app.config.WORKER_MAX_LOAD
        if args.max_load is not None:
            self._max_load = args.max_load
        if self._max_load == -1:
            self._max_load == multiprocessing.cpu_count()

        self._threads = []  # type: List[threading.Thread]
        if self._max_load:
            self._threads.append(threading.Thread(target=self._watch_load))
        if self._max_run_time:
            self._threads.append(threading.Thread(target=self._shutdown_timer))

        signals.worker_init.send(self.kuyruk, worker=self)

    def run(self) -> None:
        """Runs the worker and consumes messages from RabbitMQ.
        Returns only after `shutdown()` is called.

        """
        if self._logging_level:
            logging.basicConfig(
                level=getattr(logging, self._logging_level.upper()),
                format="%(levelname).1s %(name)s.%(funcName)s:%(lineno)d - %(message)s")

        signal.signal(signal.SIGINT, self._handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        if platform.system() != 'Windows':
            # These features will not be available on Windows, but that is OK.
            # Read this issue for more details:
            # https://github.com/cenkalti/kuyruk/issues/54
            signal.signal(signal.SIGHUP, self._handle_sighup)
            signal.signal(signal.SIGUSR1, self._handle_sigusr1)
            signal.signal(signal.SIGUSR2, self._handle_sigusr2)

        self._started_at = os.times().elapsed

        for t in self._threads:
            t.start()

        try:
            signals.worker_start.send(self.kuyruk, worker=self)
            self._consume_messages()
            signals.worker_shutdown.send(self.kuyruk, worker=self)
        finally:
            self.shutdown_pending.set()
            for t in self._threads:
                t.join()

        logger.debug("End run worker")

    def _consume_messages(self) -> None:
        with self.kuyruk.channel() as ch:
            # Set prefetch count to 1. If we don't set this, RabbitMQ keeps
            # sending messages while we are already working on a message.
            ch.basic_qos(0, 1, True)

            self._declare_queues(ch)
            self._consume_queues(ch)
            logger.info('Consumer started')
            self._main_loop(ch)

    def _main_loop(self, ch: amqp.Channel) -> None:
        while not self.shutdown_pending.is_set():
            self._pause_or_resume(ch)
            try:
                ch.connection.heartbeat_tick()
                ch.connection.drain_events(timeout=1)
            except socket.timeout:
                pass

    def _consumer_tag(self, queue: str) -> str:
        return "%s:%s@%s" % (queue, self._pid, self._hostname)

    def _declare_queues(self, ch: amqp.Channel) -> None:
        for queue in self.queues:
            logger.debug("queue_declare: %s", queue)
            ch.queue_declare(queue=queue, durable=True, auto_delete=False)

    def _pause_or_resume(self, channel: amqp.Channel) -> None:
        if not self._max_load:
            return

        try:
            load = self._current_load
        except AttributeError:
            should_pause = False
        else:
            should_pause = load > self._max_load

        if should_pause and self.consuming:
            logger.warning('Load is above the treshold (%.2f/%s), ' 'pausing consumer', load, self._max_load)
            self._cancel_queues(channel)
        elif not should_pause and not self.consuming:
            logger.warning('Load is below the treshold (%.2f/%s), ' 'resuming consumer', load, self._max_load)
            self._consume_queues(channel)

    def _consume_queues(self, ch: amqp.Channel) -> None:
        self.consuming = True
        for queue in self.queues:
            logger.debug("basic_consume: %s", queue)
            ch.basic_consume(queue=queue, consumer_tag=self._consumer_tag(queue), callback=self._process_message)

    def _cancel_queues(self, ch: amqp.Channel) -> None:
        self.consuming = False
        for queue in self.queues:
            logger.debug("basic_cancel: %s", queue)
            ch.basic_cancel(self._consumer_tag(queue))

    def _process_message(self, message: amqp.Message) -> None:
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

    def _process_description(self, message: amqp.Message, description: Dict[str, Any]) -> None:
        try:
            task = self._import_task(description['module'], description['function'])
            args, kwargs = description['args'], description['kwargs']
        except Exception:
            logger.error('Cannot import task')
            exc_info = sys.exc_info()
            signals.worker_failure.send(self.kuyruk, description=description, exc_info=exc_info, worker=self)
            message.channel.basic_reject(message.delivery_tag, requeue=False)
        else:
            self._process_task(message, description, task, args, kwargs)

    def _import_task(self, module: str, function: str) -> Task:
        if (module, function) in self._tasks:
            return self._tasks[(module, function)]

        task = importer.import_object(module, function)
        self._tasks[(module, function)] = task
        return task

    def _process_task(
            self,
            message: amqp.Message,
            description: Dict[str, Any],
            task: Task,
            args: Tuple,
            kwargs: Dict[str, Any],
    ) -> None:
        queue = message.delivery_info['routing_key']
        reply_to = message.properties.get('reply_to')
        try:
            result = self._run_task(message.channel.connection, task, args, kwargs)
        except Reject:
            logger.warning('Task is rejected')
            message.channel.basic_reject(message.delivery_tag, requeue=True)
        except Discard:
            logger.warning('Task is discarded')
            message.channel.basic_reject(message.delivery_tag, requeue=False)
            if reply_to:
                exc_info = sys.exc_info()
                self._send_reply(reply_to, message.channel, None, exc_info)
        except HeartbeatError as e:
            logger.error('Error while sending heartbeat')
            exc_info = e.exc_info
            logger.error(''.join(traceback.format_exception(*exc_info)))
            signals.worker_failure.send(
                self.kuyruk,
                description=description,
                task=task,
                args=args,
                kwargs=kwargs,
                exc_info=exc_info,
                worker=self,
                queue=queue)
            raise
        except Exception:
            logger.error('Task raised an exception')
            exc_info = sys.exc_info()
            logger.error(''.join(traceback.format_exception(*exc_info)))
            signals.worker_failure.send(
                self.kuyruk,
                description=description,
                task=task,
                args=args,
                kwargs=kwargs,
                exc_info=exc_info,
                worker=self,
                queue=queue)
            message.channel.basic_reject(message.delivery_tag, requeue=False)
            if reply_to:
                self._send_reply(reply_to, message.channel, None, exc_info)
        else:
            logger.info('Task is successful')
            message.channel.basic_ack(message.delivery_tag)
            if reply_to:
                self._send_reply(reply_to, message.channel, result, None)
        finally:
            logger.debug("Task is processed")

    def _run_task(self, connection: amqp.Connection, task: Task, args: Tuple, kwargs: Dict[str, Any]) -> Any:
        hb = Heartbeat(connection, self._on_heartbeat_error)
        hb.start()

        self.current_task = task
        self.current_args = args
        self.current_kwargs = kwargs
        try:
            return self._apply_task(task, args, kwargs)
        finally:
            self.current_task = None
            self.current_args = None
            self.current_kwargs = None

            hb.stop()

    def _on_heartbeat_error(self, exc_info: ExcInfoType) -> None:
        self._heartbeat_exc_info = exc_info
        os.kill(os.getpid(), signal.SIGHUP)

    @staticmethod
    def _apply_task(task: Task, args: Tuple, kwargs: Dict[str, Any]) -> Any:
        """Logs the time spent while running the task."""
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        start = monotonic()
        try:
            return task.apply(*args, **kwargs)
        finally:
            delta = monotonic() - start
            logger.info("%s finished in %i seconds." % (task.name, delta))

    def _send_reply(
            self,
            reply_to: str,
            channel: amqp.Channel,
            result: Any,
            exc_info: ExcInfoType,
    ) -> None:
        logger.debug("Sending reply result=%r", result)

        reply = {'result': result}
        if exc_info:
            reply['exception'] = self._exc_info_dict(exc_info)

        try:
            body = json.dumps(reply)
        except Exception as e:
            logger.error('Cannot serialize result as JSON: %s', e)
            exc_info = sys.exc_info()
            reply = {'result': None, 'exception': self._exc_info_dict(exc_info)}
            body = json.dumps(reply)

        msg = amqp.Message(body=body)
        channel.basic_publish(msg, exchange="", routing_key=reply_to)

    @staticmethod
    def _exc_info_dict(exc_info: ExcInfoType) -> Dict[str, str]:
        type_, val, tb = exc_info
        return {
            'type': '%s.%s' % (type_.__module__, type_.__name__),
            'value': str(val),
            'traceback': ''.join(traceback.format_tb(tb)),
        }

    def _watch_load(self) -> None:
        """Pause consuming messages if lood goes above the allowed limit."""
        while not self.shutdown_pending.wait(1):
            self._current_load = os.getloadavg()[0]

    @property
    def uptime(self) -> float:
        if not self._started_at:
            return 0

        return os.times().elapsed - self._started_at

    def _shutdown_timer(self) -> None:
        """Counts down from MAX_WORKER_RUN_TIME. When it reaches zero sutdown
        gracefully.

        """
        remaining = self._max_run_time - self.uptime
        if not self.shutdown_pending.wait(remaining):
            logger.warning('Run time reached zero')
            self.shutdown()

    def shutdown(self) -> None:
        """Exits after the current task is finished."""
        logger.warning("Shutdown requested")
        self.shutdown_pending.set()

    def _handle_sigint(self, signum: int, frame: Any) -> None:
        """Shutdown after processing current task."""
        logger.warning("Catched SIGINT")
        self.shutdown()

    def _handle_sigterm(self, signum: int, frame: Any) -> None:
        """Shutdown after processing current task."""
        logger.warning("Catched SIGTERM")
        self.shutdown()

    def _handle_sighup(self, signum: int, frame: Any) -> None:
        """Used internally to fail the task when connection to RabbitMQ is
        lost during the execution of the task.

        """
        logger.warning("Catched SIGHUP")
        exc_info = self._heartbeat_exc_info
        self._heartbeat_exc_info = None
        # Format exception info to see in tools like Sentry.
        formatted_exception = ''.join(traceback.format_exception(*exc_info))  # noqa
        raise HeartbeatError(exc_info)

    @staticmethod
    def _handle_sigusr1(signum: int, frame: Any) -> None:
        """Print stacktrace."""
        print('=' * 70)
        print(''.join(traceback.format_stack()))
        print('-' * 70)

    def _handle_sigusr2(self, signum: int, frame: Any) -> None:
        """Drop current task."""
        logger.warning("Catched SIGUSR2")
        if self.current_task:
            logger.warning("Dropping current task...")
            raise Discard

    def drop_task(self) -> None:
        os.kill(os.getpid(), signal.SIGUSR2)
