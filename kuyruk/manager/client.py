import socket
import logging
import threading
from functools import partial

from kuyruk.manager.messaging import message_loop

logger = logging.getLogger(__name__)


class ManagerClientThread(threading.Thread):

    def __init__(self, host, port, actor, generate_message_func):
        super(ManagerClientThread, self).__init__()
        self.host = host
        self.port = port
        self.daemon = True
        self.on_message = partial(on_message, actor)
        self.generate_message_func = generate_message_func

    def run(self):
        """Connect to manager and read/write messages from/to socket."""
        logger.debug("Running manager client thread")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.host, self.port))
            logger.debug("Connected to manager")
            message_loop(
                sock,
                self.generate_message_func,
                self.on_message)
        finally:
            logger.debug("Closing socket")
            sock.close()


def on_message(actor, sock, action):
    """Run the function sent by the manager."""
    f, args, kwargs = action
    print f, args, kwargs
    f = getattr(actor, f)
    f(*args, **kwargs)
