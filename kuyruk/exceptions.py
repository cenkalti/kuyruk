class Reject(Exception):
    """
    The task should raise this if it does not want to process the message.
    In this case message will be requeued and delivered to another worker.

    """
    pass
