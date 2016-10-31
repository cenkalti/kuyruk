import json
import errno
import socket
import logging

from monotonic import monotonic

from kuyruk.exceptions import ResultTimeout, RemoteException

logger = logging.getLogger(__name__)


class Result(object):

    def __init__(self, connection):
        self._connection = connection

    def process_message(self, message):
        logger.debug("Reply received: %s", message.body)
        d = json.loads(message.body)
        self.result = d['result']
        self.exception = d.get('exception')

    def wait(self, timeout):
        logger.debug("Waiting for task result")

        start = monotonic()

        while True:
            try:
                if self.exception:
                    raise RemoteException(self.exception['type'],
                                          self.exception['value'],
                                          self.exception['traceback'])
                return self.result
            except AttributeError:
                pass

            try:
                self._connection.heartbeat_tick()
                self._connection.drain_events(timeout=1)
            except socket.timeout:
                if monotonic() - start > timeout:
                    raise ResultTimeout
            except socket.error as e:
                if e.errno != errno.EINTR:
                    raise
