import socket
import logging
import threading

from kuyruk.manager.messaging import message_loop
from kuyruk.helpers import retry


logger = logging.getLogger(__name__)


class ManagerClientThread(threading.Thread):

    def __init__(self, host, port, generate_message, on_message,
                 socket_lock=None):
        super(ManagerClientThread, self).__init__()
        self.lock = socket_lock
        self.daemon = True
        self.host = host
        self.port = port
        self.generate_message = generate_message
        self.on_message = on_message
        self.run = retry()(self.run)

    def run(self):
        """Connect to manager and read/write messages from/to socket."""
        logger.debug("Running manager client thread")
        if self.lock:
            self.lock.acquire()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            logger.debug("Connecting to manager")
            sock.connect((self.host, self.port))
            logger.info("Connected to manager")
            if self.lock:
                self.lock.release()

            try:
                message_loop(sock, self.generate_message, self.on_message)
            except Exception:
                logger.warning("Disconnected from manager")
                raise
        finally:
            logger.debug("Closing socket")
            sock.close()
            if self.lock and self.lock.locked():
                self.lock.release()
