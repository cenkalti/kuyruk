import sys
import logging
from contextlib import contextmanager

import amqp

from kuyruk import exceptions
from kuyruk.task import Task
from kuyruk.config import Config
from kuyruk.worker import Worker

__version__ = '8.2.0'
__all__ = ['Kuyruk', 'Config', 'Task', 'Worker']


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
        self.extensions = {}

    def task(self, queue='kuyruk', **kwargs):
        """
        Wrap functions with this decorator to convert them to *tasks*.
        After wrapping, calling the function will send a message to
        a queue instead of running the function.

        :param queue: Queue name for the tasks.
        :param kwargs: Keyword arguments will be passed to
            :class:`~kuyruk.Task` constructor.
        :return: Callable :class:`~kuyruk.Task` object wrapping the original
            function.

        """
        def decorator():
            def inner(f):
                # Function may be wrapped with no-arg decorator
                queue_ = 'kuyruk' if callable(queue) else queue

                return Task(f, self, queue_, **kwargs)
            return inner

        if callable(queue):
            # task without args
            return decorator()(queue)
        else:
            # task with args
            return decorator()

    @contextmanager
    def channel(self):
        """Returns a new channel from a new connection as a context manager.
        """
        with self.connection() as conn:
            ch = conn.channel()
            logger.info('Opened new channel')
            with _safe_close(ch):
                yield ch

    @contextmanager
    def connection(self):
        """Returns a new connection as a context manager."""
        conn = amqp.Connection(
            host="%s:%s" % (self.config.RABBIT_HOST, self.config.RABBIT_PORT),
            userid=self.config.RABBIT_USER,
            password=self.config.RABBIT_PASSWORD,
            virtual_host=self.config.RABBIT_VIRTUAL_HOST,
            connect_timeout=self.config.RABBIT_CONNECT_TIMEOUT,
            read_timeout=self.config.RABBIT_READ_TIMEOUT,
            write_timeout=self.config.RABBIT_WRITE_TIMEOUT,
        )

        # from amqp==2.0.0 explicit connect is required.
        try:
            conn.connect()
        except AttributeError:
            pass

        logger.info('Connected to RabbitMQ')
        with _safe_close(conn):
            yield conn


@contextmanager
def _safe_close(obj):
    try:
        yield
    except Exception:
        # Error occurred in block. Save exception info for re-raising later.
        exc_info = sys.exc_info()

        # We still need to close the object but not interested with errors,
        # because we will raise the original exception above.
        try:
            obj.close()
        except Exception:
            pass

        # After closing the object, we are re-raising the saved exception.
        if sys.version_info.major == 3:
            raise exc_info[1].with_traceback(exc_info[2])
        else:
            exec("raise exc_info[0], exc_info[1], exc_info[2]")
    else:
        # No error occurred in block. We must close the object as usual.
        obj.close()
