import socket
import logging
import threading
from kuyruk.manager.messaging import message_loop
from kuyruk.helpers import retry

logger = logging.getLogger(__name__)


class ManagerClientThread(threading.Thread):

    def __init__(self, host, port, generate_message, on_message, stop_event):
        super(ManagerClientThread, self).__init__()
        self.daemon = True
        self.host = host
        self.port = port
        self.generate_message = generate_message
        self.on_message = on_message
        self.stop_event = stop_event
        self.run = retry(stop_event=stop_event)(self.run)

    def run(self):
        """Connect to manager and read/write messages from/to socket."""
        logger.debug("Running manager client thread")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            logger.debug("Connection to manager")
            sock.connect((self.host, self.port))
            logger.debug("Connected to manager")
            message_loop(sock,
                         self.generate_message, self.on_message,
                         stop_event=self.stop_event)
        finally:
            logger.debug("Closing socket")
            sock.close()
