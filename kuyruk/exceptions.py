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


class ConnectionError(KuyrukError):
    pass
