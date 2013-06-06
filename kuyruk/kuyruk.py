from __future__ import absolute_import
import logging
from contextlib import contextmanager

import pika
import pika.exceptions

import kuyruk.exceptions
from kuyruk.task import Task
from kuyruk.config import Config
from kuyruk.events import EventMixin

logger = logging.getLogger(__name__)


class Kuyruk(EventMixin):
    """
    Main class for Kuyruk distributed task queue. It holds the configuration
    values and provides a task decorator for user application

    :param config: A module that contains configuration options.
                   See :ref:`configuration-options` for default values.

    """
    Reject = kuyruk.exceptions.Reject  # Shortcut for raising from tasks

    def __init__(self, config=None, task_class=Task):
        self.task_class = task_class
        self.config = Config()
        self._connection = None
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
        :param retry: Retry this times before give up.
        :param task_class: Custom task class.
            Must be a subclass of :class:`~Task`.
            If this is :const:`None` then :attr:`Task.task_class` will be used.
        :param max_run_time: Maximum allowed time in seconds for task to
            complete.
        :param arg_class: Class of the first argument. If it is present,
            the first argument will be converted to it's ``id`` when sending the
            task to the queue and it will be reloaded on worker when running
            the task.
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

    @property
    def connection(self):
        """Persistent connection object of instance."""
        if self._connection is None:
            self._connection = self._connect()
        return self._connection

    def _connect(self):
        """Returns new connection object."""
        parameters = pika.ConnectionParameters(
            host=self.config.RABBIT_HOST,
            port=self.config.RABBIT_PORT,
            virtual_host=self.config.RABBIT_VIRTUAL_HOST,
            credentials=pika.PlainCredentials(
                self.config.RABBIT_USER,
                self.config.RABBIT_PASSWORD),
            heartbeat_interval=600,
            socket_timeout=2,
            connection_attempts=2)
        connection = pika.BlockingConnection(parameters)
        logger.info('Connected to RabbitMQ')
        return connection

    def _channel(self):
        """Returns new channel object."""
        CLOSED = (pika.exceptions.ConnectionClosed,
                  pika.exceptions.ChannelClosed)

        try:
            return self.connection.channel()
        except CLOSED:
            logger.warning("Connection is closed. Reconnecting...")
            try:
                self._connection.close()
            except CLOSED:
                logger.debug("Connection is already closed.")

            self._connection = self._connect()
            return self._connection.channel()

    @contextmanager
    def channel(self):
        """
        Yields a new channel object.
        When exiting the context the channel will be closed.

        """
        ch = self._channel()
        try:
            yield ch
        finally:
            try:
                ch.close()
            except pika.exceptions.ChannelClosed:
                logger.debug("Channel is already closed.")
