from __future__ import absolute_import
import atexit
import logging
from threading import RLock
from contextlib import contextmanager, closing

import amqp

from kuyruk import exceptions, importer
from kuyruk.task import Task
from kuyruk.config import Config
from kuyruk.worker import Worker

__version__ = '2.0.2'
__all__ = ['Kuyruk', 'Config', 'Task']


logger = logging.getLogger(__name__)


# Add NullHandler to prevent logging warnings on startup
null_handler = logging.NullHandler()
logger.addHandler(null_handler)


class Kuyruk(object):
    """
    Provides :func:`~kuyruk.Kuyruk.task` decorator to convert a function
    into a :class:`~kuyruk.Task`.

    Provides :func:`~kuyruk.Kuyruk.channel` context manager for opening a
    new channel on the connection.
    Connection is opened when the first channel is created.

    Maintains a single connection to RabbitMQ server.
    If you use the :class:`~kuyruk.Kuyruk` object as a context manager,
    the connection will be closed when exiting.

    :param config: Must be an instance of :class:`~kuyruk.Config`.
                   If ``None``, default config is used.
                   See :class:`~kuyruk.Config` for default values.

    """
    # Aliases for raising from tasks
    Reject = exceptions.Reject
    Discard = exceptions.Discard

    def __init__(self, config=None):
        if config is None:
            config = Config()
        if not isinstance(config, Config):
            raise TypeError
        self.config = config
        self._connection = None
        self._lock = RLock()  # protects self._connection

        # Close open RabbitMQ connection at exit.
        def _close():
            try:
                self._close_connection()
            except Exception:
                pass
        atexit.register(_close)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_connection()

    def _close_connection(self):
        with self._lock:
            if self._connection is not None:
                self._connection.close()

    def task(self, queue='kuyruk', local=False, retry=0, max_run_time=None):
        """
        Wrap functions with this decorator to convert them to *tasks*.
        After wrapping, calling the function will send a message to
        a queue instead of running the function.

        :param queue: Queue name for the tasks.
        :param retry: Retry this times before give up.
            The failed task will be retried in the same worker.
        :param max_run_time: Maximum allowed time in seconds for task to
            complete.
        :param local: Append hostname to the queue name. Worker needs to be
            started on the local queue to receive this task.
        :return: Callable :class:`~kuyruk.Task` object wrapping the original
            function.

        """
        def decorator():
            def inner(f):
                # Function may be wrapped with no-arg decorator
                queue_ = 'kuyruk' if callable(queue) else queue

                return Task(
                    f, self, queue=queue_, local=local,
                    retry=retry, max_run_time=max_run_time)
            return inner

        if callable(queue):
            # task without args
            return decorator()(queue)
        else:
            # task with args
            return decorator()

    @contextmanager
    def channel(self):
        """Returns a new channel as a context manager.
        A lock will be held in the context.
        Be aware of this when using Kuyruk in a multi-threaded program.
        """
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

    def _connect(self):
        """Returns a new connection."""
        conn = amqp.Connection(
            host="%s:%s" % (self.config.RABBIT_HOST, self.config.RABBIT_PORT),
            userid=self.config.RABBIT_USER,
            password=self.config.RABBIT_PASSWORD,
            virtual_host=self.config.RABBIT_VIRTUAL_HOST)
        logger.info('Connected to RabbitMQ')
        return conn
