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
    Raised from Result.wait if reply is not received in timeout seconds.

    """
    pass


class RemoteException(KuyrukError):
    """
    Raised from Result.wait if exception is raised on the worker
    while running the task.

    """
    def __init__(self, type_, value, traceback):
        self.type = type_
        self.value = value
        self.traceback = traceback


class ConnectionError(KuyrukError):
    """
    Raised from Task.apply in worker when there is a problem while
    sending heartbeat.

    """
    pass
