from __future__ import absolute_import
import atexit
import logging
from threading import Lock
from contextlib import contextmanager, closing

import amqp

from kuyruk import exceptions
from kuyruk.task import Task
from kuyruk.config import Config
from kuyruk.worker import Worker
from kuyruk.events import EventMixin

__version__ = '1.2.4'
__all__ = ['Kuyruk', 'Config', 'Task', 'Worker']


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
        self._lock = Lock()  # protects self._connection
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
            return decorator()(queue)
        else:
            return decorator()

    @contextmanager
    def channel(self):
        """Returns a new channel."""
        with self._lock:
            # Connect once
            if self._connection is None:
                self._connection = self._connect()

            # Try to create new channel.
            # If fails try again with a new connection.
            try:
                channel = self._connection.channel()
            except Exception:
                try:
                    self._connection.close()
                except Exception:
                    pass
                self._connection = self._connect()
                channel = self._connection.channel()

            with closing(channel):
                yield channel

    def close(self):
        """Close the connection."""
        with self._lock:
            if self._connection is not None:
                self._connection.close()

    def _connect(self):
        """Returns a new connection."""
        conn = amqp.Connection(
            host="%s:%s" % (self.config.RABBIT_HOST, self.config.RABBIT_PORT),
            user_id=self.config.RABBIT_USER,
            password=self.config.RABBIT_PASSWORD,
            virtual_host=self.config.RABBIT_VIRTUAL_HOST)
        logger.info('Connected to RabbitMQ')
        return conn
