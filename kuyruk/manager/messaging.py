import json
import errno
import struct
import socket
import logging
import threading
from time import sleep

logger = logging.getLogger(__name__)


def message_loop(sock, generate_message, callback,
                 stop_event=None, sleep_seconds=1):
    """
    Send and receive messages by calling callback functions
    until stop_event is set.

    :param sock: socket object
    :param generate_message: callback function to generate message to be sent.
    :param callback: callback function for messages received.
    :param stop_event: loop will finish if this is set.
    :param sleep_seconds: wait seconds between each iteration.
    :return: None

    """
    logger.debug("Running message loop...")
    if stop_event is None:
        stop_event = threading.Event()

    sock.setblocking(0)
    while not stop_event.is_set():
        send_and_receive(sock, generate_message, callback)
        sleep(sleep_seconds)


def send_and_receive(sock, generate_message, callback):
    message = generate_message()
    if message:
        send_message(sock, message)
        logger.debug('Message sent')

    try:
        message = receive_message(sock)
        logger.debug('Message received')
    except socket.error as e:
        if e.errno != errno.EAGAIN:  # Resource temporarily unavailable
            raise
    else:
        callback(sock, message)


def send_message(sock, message):
    message = json.dumps(message)
    size = len(message)
    size = struct.pack('I', size)
    sock.sendall(size)
    sock.sendall(message)


def receive_message(sock):
    size = sock.recv(4)
    if not size:
        raise EOFError
    size = struct.unpack('I', size)[0]
    message = sock.recv(size)
    return json.loads(message)
