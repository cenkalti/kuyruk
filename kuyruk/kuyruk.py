import sys
import json
import logging
from contextlib import contextmanager
from typing import Dict, Any, Set, Callable, Iterator, List  # noqa

import amqp

from kuyruk import signals
from kuyruk.config import Config
from kuyruk.task import Task, SubTask

logger = logging.getLogger(__name__)


class Kuyruk:
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
    def __init__(self, config: Config = None) -> None:
        if config is None:
            config = Config()

        self.config = config
        self.extensions = {}  # type: Dict[str, Any]

    def task(self, queue: str = 'kuyruk', **kwargs: Any) -> Callable:
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
        def wrapper(f: Callable) -> Task:
            return Task(f, self, queue, **kwargs)

        return wrapper

    @contextmanager
    def channel(self) -> Iterator[amqp.Channel]:
        """Returns a new channel from a new connection as a context manager."""
        with self.connection() as conn:
            ch = conn.channel()
            logger.info('Opened new channel')
            with _safe_close(ch):
                yield ch

    @contextmanager
    def connection(self) -> Iterator[amqp.Connection]:
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
        conn.connect()
        logger.info('Connected to RabbitMQ')
        with _safe_close(conn):
            yield conn

    def send_tasks_to_queue(self, subtasks: List[SubTask]) -> None:
        if self.config.EAGER:
            for subtask in subtasks:
                subtask.task.apply(*subtask.args, **subtask.kwargs)
            return

        declared_queues = set()  # type: Set[str]
        with self.channel() as ch:
            for subtask in subtasks:
                queue = subtask.task._queue_for_host(subtask.host)
                if queue not in declared_queues:
                    ch.queue_declare(queue=queue, durable=True, auto_delete=False)
                    declared_queues.add(queue)

                description = subtask.task._get_description(subtask.args,
                                                            subtask.kwargs)
                subtask.task._send_signal(signals.task_presend,
                                          args=subtask.args,
                                          kwargs=subtask.kwargs,
                                          description=description)

                body = json.dumps(description)
                msg = amqp.Message(body=body)
                ch.basic_publish(msg, exchange="", routing_key=queue)
                subtask.task._send_signal(signals.task_postsend,
                                          args=subtask.args,
                                          kwargs=subtask.kwargs,
                                          description=description)


@contextmanager
def _safe_close(obj: Any) -> Iterator[None]:
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
        raise exc_info[1].with_traceback(exc_info[2])
    else:
        # No error occurred in block. We must close the object as usual.
        obj.close()
