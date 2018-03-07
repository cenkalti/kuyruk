import os
import sys
import json
import platform
import signal
import socket
import logging
from uuid import uuid1
from datetime import datetime
from contextlib import contextmanager
from typing import Callable, Tuple, Dict, Any, NamedTuple, TYPE_CHECKING, Union, Iterator

import amqp
from blinker import Signal

from kuyruk import signals, importer
from kuyruk.exceptions import Timeout
from kuyruk.result import Result

if TYPE_CHECKING:
    from kuyruk.kuyruk import Kuyruk  # noqa

logger = logging.getLogger(__name__)


SubTask = NamedTuple("SubTask", [
    ("task", 'Task'),
    ("args", Tuple),
    ("kwargs", Dict[str, Any]),
    ("host", str),
])


class Task:
    """Calling a :class:`~kuyruk.Task` object serializes the task to JSON
    and sends it to the queue.

    :param retry: Retry this times before give up.
        The failed task will be retried in the same worker.
    :param max_run_time: Maximum allowed time in seconds for task to
        complete.

    """

    def __init__(self, f: Callable, kuyruk: 'Kuyruk', queue: str, retry: int=0, max_run_time: int=None) -> None:
        self.f = f
        self.kuyruk = kuyruk
        self.queue = queue
        self.retry = retry
        self.max_run_time = max_run_time
        self._send_signal(signals.task_init)

    def __repr__(self) -> str:
        return "<Task of %r>" % self.name

    def __call__(self, *args: Tuple, **kwargs: Any) -> None:
        """When a function is wrapped with a task decorator it will be
        converted to a Task object. By overriding __call__ method we are
        sending this task to queue instead of invoking the function
        without changing the client code.

        """
        logger.debug("Task.__call__ args=%r, kwargs=%r", args, kwargs)
        self.send_to_queue(args, kwargs)

    def subtask(self, args: Tuple=(), kwargs: Dict[str, Any]={}, host: str=None) -> SubTask:
        return SubTask(self, args, kwargs, host)

    def send_to_queue(
            self,
            args: Tuple=(),
            kwargs: Dict[str, Any]={},
            host: str=None,
            wait_result: Union[int, float]=None,
            message_ttl: Union[int, float]=None,
    ) -> Any:
        """
        Sends a message to the queue.
        A worker will run the task's function when it receives the message.

        :param args: Arguments that will be passed to task on execution.
        :param kwargs: Keyword arguments that will be passed to task
            on execution.
        :param host: Send this task to specific host. ``host`` will be
            appended to the queue name. If ``host`` is "localhost", hostname
            of the server will be appended to the queue name.
        :param wait_result:
            Wait for result from worker for ``wait_result`` seconds.
            If timeout occurs,
            :class:`~kuyruk.exceptions.ResultTimeout` is raised.
            If excecption occurs in worker,
            :class:`~kuyruk.exceptions.RemoteException` is raised.
        :param message_ttl:
            If set, message will be destroyed in queue after ``message_ttl``
            seconds.
        :return: Result from worker if ``wait_result`` is set,
            else :const:`None`.

        """
        if self.kuyruk.config.EAGER:
            # Run the task in current process
            result = self.apply(*args, **kwargs)
            return result if wait_result else None

        logger.debug("Task.send_to_queue args=%r, kwargs=%r", args, kwargs)
        queue = self._queue_for_host(host)
        description = self._get_description(args, kwargs)
        self._send_signal(signals.task_presend, args=args, kwargs=kwargs, description=description)

        body = json.dumps(description)
        msg = amqp.Message(body=body)
        if wait_result:
            # Use direct reply-to feature from RabbitMQ:
            # https://www.rabbitmq.com/direct-reply-to.html
            msg.properties['reply_to'] = 'amq.rabbitmq.reply-to'

        if message_ttl:
            msg.properties['expiration'] = str(int(message_ttl * 1000))

        with self.kuyruk.channel() as ch:
            if wait_result:
                result = Result(ch.connection)
                ch.basic_consume(queue='amq.rabbitmq.reply-to', no_ack=True, callback=result.process_message)

            ch.queue_declare(queue=queue, durable=True, auto_delete=False)
            ch.basic_publish(msg, exchange="", routing_key=queue)
            self._send_signal(signals.task_postsend, args=args, kwargs=kwargs, description=description)

            if wait_result:
                return result.wait(wait_result)

    def _queue_for_host(self, host: str) -> str:
        if not host:
            return self.queue
        if host == 'localhost':
            host = socket.gethostname()
        return "%s.%s" % (self.queue, host)

    def _get_description(self, args: Tuple, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Return the dictionary to be sent to the queue."""
        return {
            'id': uuid1().hex,
            'args': args,
            'kwargs': kwargs,
            'module': self._module_name,
            'function': self.f.__name__,
            'sender_hostname': socket.gethostname(),
            'sender_pid': os.getpid(),
            'sender_cmd': ' '.join(sys.argv),
            'sender_timestamp': datetime.utcnow().isoformat()[:19],
        }

    def _send_signal(self, sig: Signal, **data: Any) -> None:
        sig.send(self.kuyruk, task=self, **data)

    def apply(self, *args: Any, **kwargs: Any) -> Any:
        """Called by workers to run the wrapped function.
        You may call it yourself if you want to run the task in current process
        without sending to the queue.

        If task has a `retry` property it will be retried on failure.

        If task has a `max_run_time` property the task will not be allowed to
        run more than that.
        """
        def send_signal(sig: Signal, **extra: Any) -> None:
            self._send_signal(sig, args=args, kwargs=kwargs, **extra)

        logger.debug("Applying %r, args=%r, kwargs=%r", self, args, kwargs)

        send_signal(signals.task_preapply)
        try:
            tries = 1 + self.retry
            while 1:
                tries -= 1
                send_signal(signals.task_prerun)
                try:
                    with time_limit(self.max_run_time or 0):
                        return self.f(*args, **kwargs)
                except Exception:
                    send_signal(signals.task_error, exc_info=sys.exc_info())
                    if tries <= 0:
                        raise
                else:
                    break
                finally:
                    send_signal(signals.task_postrun)
        except Exception:
            send_signal(signals.task_failure, exc_info=sys.exc_info())
            raise
        else:
            send_signal(signals.task_success)
        finally:
            send_signal(signals.task_postapply)

    @property
    def name(self) -> str:
        """Full path to the task in the form of `<module>.<function>`.
        Workers find and import tasks by this path.

        """
        return "%s:%s" % (self._module_name, self.f.__name__)

    @property
    def _module_name(self) -> str:
        """Module name of the wrapped function."""
        name = self.f.__module__
        if name == '__main__':
            return importer.main_module_name()
        return name


@contextmanager
def time_limit(seconds: int) -> Iterator[None]:
    if seconds == 0:
        yield
        return

    if platform.system() == 'Windows':
        raise NotImplementedError("There is no way to implement a general purpose time-limit on "
                                  "Windows. Read this issue for more details: "
                                  "https://github.com/cenkalti/kuyruk/issues/54")

    def signal_handler(signum: int, frame: Any) -> None:
        raise Timeout

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
