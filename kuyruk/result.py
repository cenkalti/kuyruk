import time
import json
import errno
import socket
import logging

from kuyruk.heartbeat import Heartbeat
from kuyruk.exceptions import ResultTimeout, RemoteException

logger = logging.getLogger(__name__)


class Result(object):

    def __init__(self, connection):
        self._connection = connection
        self._heartbeat = Heartbeat(connection)

    def _start_heartbeat(self):
        self._heartbeat.start()

    def _stop_heartbeat(self):
        self._heartbeat.stop()

    def _process_message(self, message):
        logger.debug("Reply received: %s", message.body)
        d = json.loads(message.body)
        self.result = d['result']
        self.exception = d.get('exception')

    def wait(self, timeout):
        logger.debug("Waiting for task result")

        start = time.time()
        self._stop_heartbeat()

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
                if time.time() - start > timeout:
                    raise ResultTimeout
            except socket.error as e:
                if e.errno != errno.EINTR:
                    raise
