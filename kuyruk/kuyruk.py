import json
import logging
from contextlib import contextmanager, closing
from typing import Dict, Any, Set, Callable, Iterator, List

import amqp

from kuyruk import signals
from kuyruk.config import Config
from kuyruk.task import Task, SubTask
from kuyruk.connection import SingleConnection

logger = logging.getLogger(__name__)


class Kuyruk:
    """
    Provides :func:`~kuyruk.Kuyruk.task` decorator to convert a function
    into a :class:`~kuyruk.Task`.

    Provides :func:`~kuyruk.Kuyruk.channel` context manager for opening a
    new channel on the connection.
    Connection is opened when the first channel is created and kept open.

    :param config: Must be an instance of :class:`~kuyruk.Config`.
                   If ``None``, default config is used.
                   See :class:`~kuyruk.Config` for default values.

    """

    def __init__(self, config: Config = None) -> None:
        if config is None:
            config = Config()

        self.config = config
        self.extensions: Dict[str, Any] = {}
        self._connection = self._new_connection()

    def task(self, queue: str = 'kuyruk', **kwargs: Any) -> Callable:
        """
        Wrap functions with this decorator to convert them to *tasks*.
        After wrapping, calling the function will send a message to a queue instead of running the function.

        :param queue: Queue name for the tasks.
        :param kwargs: Keyword arguments will be passed to :class:`~kuyruk.Task` constructor.
        :return: Callable :class:`~kuyruk.Task` object wrapping the original
            function.

        """
        def wrapper(f: Callable) -> Task:
            return Task(f, self, queue, **kwargs)

        return wrapper

    @contextmanager
    def connection(self) -> Iterator[amqp.Connection]:
        """Returns underlying AMQP connection as a context manager.
        Connection object is locked while in context and cannot be used by other threads.
        If you need a connection for a longer duration, use :class:`~kuyruk.Kuyruk.new_connection` method."""
        with self._connection as connection:
            yield connection

    @contextmanager
    def new_connection(self) -> Iterator[amqp.Connection]:
        """Returns new AMQP connection as a context manager.
        Connection is closed when context manager exits."""
        conn: amqp.Connection = self._new_connection().new_connection()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def channel(self) -> Iterator[amqp.Channel]:
        """Returns a new channel created on the uderlying connection as a context manager."""
        with self._connection as connection:
            channel = connection.channel()
            logger.info('Opened new channel')
            with closing(channel):
                yield channel

    def _new_connection(self) -> SingleConnection:
        return SingleConnection(
            host=self.config.RABBIT_HOST,
            port=self.config.RABBIT_PORT,
            vhost=self.config.RABBIT_VIRTUAL_HOST,
            user=self.config.RABBIT_USER,
            password=self.config.RABBIT_PASSWORD,
            ssl=self.config.RABBIT_SSL,
            heartbeat=self.config.RABBIT_HEARTBEAT,
            max_idle_duration=self.config.RABBIT_IDLE_DURATION,
            connect_timeout=self.config.RABBIT_CONNECT_TIMEOUT,
            read_timeout=self.config.RABBIT_READ_TIMEOUT,
            write_timeout=self.config.RABBIT_WRITE_TIMEOUT,
            tcp_user_timeout=self.config.TCP_USER_TIMEOUT,
        )

    def send_tasks_to_queue(self, subtasks: List[SubTask]) -> None:
        if self.config.EAGER:
            for subtask in subtasks:
                subtask.task.apply(*subtask.args, **subtask.kwargs)
            return

        declared_queues: Set[str] = set()
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
