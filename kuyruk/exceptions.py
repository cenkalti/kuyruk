from typing import Tuple, Type
from types import TracebackType

ExcInfoType = Tuple[Type[BaseException], BaseException, TracebackType]


class KuyrukError(Exception):
    """Base class for Kuyruk exceptions."""
    pass


class Reject(KuyrukError):
    """
    The task may raise this if it does not want to process the message.
    The message will be requeued and delivered to another worker.

    """
    pass


class Discard(KuyrukError):
    """
    The task may raise this if it does not want to process the message.
    The message will be dropped.

    """
    pass


class Timeout(KuyrukError):
    """Raised if a task exceeds it's allowed run time."""
    pass


class ResultTimeout(KuyrukError):
    """
    Raised from :func:`kuyruk.Task.send_to_queue` if ``wait_result`` is set and
    reply is not received in ``wait_result`` seconds.

    """
    pass


class RemoteException(KuyrukError):
    """
    Raised from :func:`kuyruk.Task.send_to_queue` if ``wait_result`` is set and
    exception is raised on the worker while running the task.

    """
    def __init__(self, type_: Type, value: Exception, traceback: TracebackType) -> None:
        self.type = type_
        self.value = value
        self.traceback = traceback

    def __str__(self) -> str:
        return "%s(%r)" % (self.type, self.value)


class HeartbeatError(KuyrukError):
    """
    Raised when there is problem while sending heartbeat during task execution.

    """
    def __init__(self, exc_info: ExcInfoType) -> None:
        self.exc_info = exc_info
