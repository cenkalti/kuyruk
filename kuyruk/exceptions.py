class KuyrukError(Exception):
    """Base class for Kuyruk related exceptions."""
    pass


class Reject(KuyrukError):
    """
    The task should raise this if it does not want to process the message.
    In this case message will be requeued and delivered to another worker.

    """
    pass


class ObjectNotFound(KuyrukError):
    """Internal exception that is raised
    when the worker cannot fetch the object of a class task."""
    pass
