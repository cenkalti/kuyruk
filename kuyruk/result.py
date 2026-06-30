import json
import socket
import logging
from time import monotonic
from typing import Any, Union

import amqp

from kuyruk.exceptions import ResultTimeout, RemoteException

logger = logging.getLogger(__name__)


class Result:

    def __init__(self, connection: amqp.Connection) -> None:
        self._connection = connection
        self.exception = None
        self.result = None
        self._received = False

    def process_message(self, message: amqp.Message) -> None:
        logger.debug("Reply received: %s", message.body)
        d = json.loads(message.body)
        self.result = d['result']
        self.exception = d.get('exception')
        self._received = True

    def wait(self, timeout: Union[float, int]) -> Any:
        logger.debug("Waiting for task result")

        start = monotonic()
        while True:
            if self._received:
                if self.exception:
                    raise RemoteException(self.exception['type'],
                                          self.exception['value'],
                                          self.exception['traceback'])
                return self.result

            if monotonic() - start > timeout:
                raise ResultTimeout

            try:
                self._connection.heartbeat_tick()
                self._connection.drain_events(timeout=1)
            except socket.timeout:
                pass
