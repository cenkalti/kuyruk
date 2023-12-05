import sys
import json
import logging
import threading
from contextlib import contextmanager, closing, suppress
from typing import Dict, Any, Set, Callable, Iterator, List, Optional

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

        self._connection: Optional[amqp.Connection] = None
        self._connection_lock = threading.RLock()

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
            with closing(ch):
                yield ch

    @contextmanager
    def connection(self) -> Iterator[amqp.Connection]:
        """Returns a new connection as a context manager."""
        with self._connection_lock:
            conn = self._get_connection()
            if conn is None:
                conn = self._new_connection()
                logger.info('Connected to RabbitMQ')

            yield conn

    def _get_connection(self) -> Optional[amqp.Connection]:
        if self._connection is None:
            return None

        if not self._connection_is_alive():
            self._remove_connection()
            return None

        return self._connection

    def _remove_connection(self) -> None:
        if self._connection is None:
            return

        with suppress(Exception):
            self._connection.close()

        self._connection = None

    def _connection_is_alive(self) -> bool:
        if self._connection is None:
            return False

        try:
            self._connection.send_heartbeat()
        except IOError:
            return False
        else:
            return True

    def _new_connection(self) -> amqp.Connection:
        socket_settings = {}
        if sys.platform.startswith('linux'):
            TCP_USER_TIMEOUT = 18  # constant is available on Python 3.6+.
            socket_settings[TCP_USER_TIMEOUT] = self.config.TCP_USER_TIMEOUT * 1000

        conn = amqp.Connection(
            host="%s:%s" % (self.config.RABBIT_HOST, self.config.RABBIT_PORT),
            userid=self.config.RABBIT_USER,
            password=self.config.RABBIT_PASSWORD,
            virtual_host=self.config.RABBIT_VIRTUAL_HOST,
            connect_timeout=self.config.RABBIT_CONNECT_TIMEOUT,
            read_timeout=self.config.RABBIT_READ_TIMEOUT,
            write_timeout=self.config.RABBIT_WRITE_TIMEOUT,
            socket_settings=socket_settings,
            heartbeat=self.config.RABBIT_HEARTBEAT,
            ssl=self.config.RABBIT_SSL,
        )
        conn.connect()

        self._connection = conn
        return conn

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
