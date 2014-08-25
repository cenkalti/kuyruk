from __future__ import absolute_import
import errno
import select
import logging

import pika
import pika.exceptions

from kuyruk import exceptions
from kuyruk.task import Task
from kuyruk.queue import Queue
from kuyruk.config import Config
from kuyruk.worker import Worker
from kuyruk.events import EventMixin
from kuyruk.connection import Connection

__version__ = '1.2.1'
__all__ = ['Kuyruk', 'Task', 'Worker']


logger = logging.getLogger(__name__)


# Add NullHandler to prevent logging warnings on startup
null_handler = logging.NullHandler()
logger.addHandler(null_handler)


# Pika should do this. Patch submitted to Pika.
logging.getLogger('pika').addHandler(null_handler)


# Monkey patch ReadPoller until pika 0.9.14 is released
def ready(self):
    while True:
        try:
            return original_method(self)
        except select.error as e:
            if e[0] != errno.EINTR:
                raise
from pika.adapters.blocking_connection import ReadPoller
original_method = ReadPoller.ready
ReadPoller.ready = ready


class Kuyruk(EventMixin):
    """
    Main class for Kuyruk distributed task queue. It holds the configuration
    values and provides a task decorator for user application

    :param config: A module that contains configuration options.
                   See :ref:`configuration-options` for default values.

    """
    Reject = exceptions.Reject  # Shortcut for raising from tasks

    def __init__(self, config=None, task_class=Task):
        self.task_class = task_class
        self.config = Config()
        self._connection = None
        self._channel = None
        if config:
            self.config.from_object(config)

    def task(self, queue='kuyruk', eager=False, retry=0, task_class=None,
             max_run_time=None, local=False, arg_class=None):
        """
        Wrap functions with this decorator to convert them to background
        tasks. After wrapping, calling the function will send a message to
        queue instead of running the function.

        :param queue: Queue name for the tasks.
        :param eager: Run task in process, do not use RabbitMQ.
        :param retry: Retry this times before give up. Task will be re-routed
            and executed on another worker.
        :param task_class: Custom task class.
            Must be a subclass of :class:`~Task`.
            If this is :const:`None` then :attr:`Task.task_class` will be used.
        :param max_run_time: Maximum allowed time in seconds for task to
            complete.
        :param arg_class: Class of the first argument. If it is present,
            the first argument will be converted to it's ``id`` when sending
            the task to the queue and it will be reloaded on worker when
            running the task.
        :return: Callable :class:`~Task` object wrapping the original function.

        """
        def decorator():
            def inner(f):
                # Function may be wrapped with no-arg decorator
                queue_ = 'kuyruk' if callable(queue) else queue

                task_class_ = task_class or self.task_class
                return task_class_(
                    f, self,
                    queue=queue_, eager=eager, local=local, retry=retry,
                    max_run_time=max_run_time, arg_class=arg_class)
            return inner

        if callable(queue):
            logger.debug('task without args')
            return decorator()(queue)
        else:
            logger.debug('task with args')
            return decorator()

    def connection(self):
        """
        Returns the shared RabbitMQ connection.
        Creates a new connection if it is not connected.

        """
        if self._connection is None or not self._connection.is_open:
            self._connection = self._connect()

        return self._connection

    def channel(self):
        """
        Returns the shared channel.
        Creates a new channel if there is no available.

        """
        if self._channel is None or not self._channel.is_open:
            self._channel = self._open_channel()

        return self._channel

    def close(self):
        if self._connection is not None:
            if self._connection.is_open:
                self._connection.close()

    def _connect(self):
        """Returns new connection object."""
        parameters = pika.ConnectionParameters(
            host=self.config.RABBIT_HOST,
            port=self.config.RABBIT_PORT,
            virtual_host=self.config.RABBIT_VIRTUAL_HOST,
            credentials=pika.PlainCredentials(
                self.config.RABBIT_USER,
                self.config.RABBIT_PASSWORD),
            heartbeat_interval=0,  # We don't want heartbeats
            socket_timeout=2,
            connection_attempts=2)
        connection = Connection(parameters)
        logger.info('Connected to RabbitMQ')
        return connection

    def _open_channel(self):
        """Returns a new channel."""
        CLOSED = (pika.exceptions.ConnectionClosed,
                  pika.exceptions.ChannelClosed)

        try:
            channel = self.connection().channel()
        except CLOSED:
            logger.warning("Connection is closed. Reconnecting...")

            # If there is a connection, try to close it
            if self._connection:
                try:
                    self._connection.close()
                except CLOSED:
                    pass

            self._connection = self._connect()
            channel = self._connection.channel()

        return channel
