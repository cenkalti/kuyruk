import sys
import socket
import logging
import threading
from typing import Callable

import amqp

from kuyruk.exceptions import ExcInfoType

logger = logging.getLogger(__name__)


class Heartbeat:

    def __init__(self, connection: amqp.Connection, on_error: Callable[[ExcInfoType], None]) -> None:
        self._connection = connection
        self._on_error = on_error
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._connection.heartbeat_tick()
                self._connection.drain_events(timeout=1)
            except socket.timeout:
                pass
            except Exception as e:
                logger.exception("cannot send heartbeat: %s", e)
                self._on_error(sys.exc_info())
                break
