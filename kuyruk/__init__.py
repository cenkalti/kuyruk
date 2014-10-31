from __future__ import absolute_import
import atexit
import logging
from threading import Lock

import rabbitpy

from kuyruk import exceptions
from kuyruk.task import Task
from kuyruk.config import Config
from kuyruk.worker import Worker
from kuyruk.events import EventMixin

__version__ = '1.2.4'
__all__ = ['Kuyruk', 'Task', 'Worker']


logger = logging.getLogger(__name__)


# Add NullHandler to prevent logging warnings on startup
null_handler = logging.NullHandler()
logger.addHandler(null_handler)


class Kuyruk(EventMixin):
    """
    Main class for Kuyruk distributed task queue. It holds the configuration
    values and provides a task decorator for user application

    :param config: A module that contains configuration options.
                   See :ref:`configuration-options` for default values.

    """
    Reject = exceptions.Reject  # Shortcut for raising from tasks

    def __init__(self, config=None, task_class=Task):
        if config is None:
            config = Config()
        if not isinstance(config, Config):
            raise TypeError
        self.config = config
        self.task_class = task_class
        self._connection = None
        self._connection_lock = Lock()
        self._channel = None
        self._channel_lock = Lock()
        atexit.register(self.close)

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

    def close(self):
        """Close RabbitMQ connection if open."""
        with self._connection_lock:
            if self._connection is not None:
                self._connection.close()

    @property
    def connection(self):
        """
        Returns the shared RabbitMQ connection.
        Creates a new connection if it is not connected.

        """
        with self._connection_lock:
            if self._connection is None or self._connection.closed:
                self._connection = self._connect()
            return self._connection

    def _connect(self):
        logger.info("Connection to RabbitMQ...")
        connection = rabbitpy.Connection(
            "amqp://{user}:{password}@{host}:{port}/{virtual_host}"
            "?heartbeat=0&connection_timeout=2".format(
                user=self.config.RABBIT_USER,
                password=self.config.RABBIT_PASSWORD,
                host=self.config.RABBIT_HOST,
                port=self.config.RABBIT_PORT,
                virtual_host=self.config.RABBIT_VIRTUAL_HOST))
        logger.info('Connected to RabbitMQ')
        return connection

    def channel(self):
        """Returns a new channel. Don't forget to close when you are done."""
        logger.info("Opening new channel...")
        ch = self.connection.channel()
        logger.info("Opened new channel")
        return ch

    @property
    def _channel_for_publishing(self):
        """This function is used to get the channel when sending tasks to
        queues."""
        with self._channel_lock:
            if self._channel is None or self._channel.closed:
                self._channel = self.channel()
                # We will use mandatory flag when publishing.
                self._channel.enable_publisher_confirms()
            return self._channel
