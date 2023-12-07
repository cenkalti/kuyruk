import socket
import logging
import threading
from typing import Callable

import amqp

logger = logging.getLogger(__name__)


class Heartbeat:

    def __init__(self, connection: amqp.Connection, on_error: Callable[[Exception], None]) -> None:
        self._connection = connection
        self._on_error = on_error
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        """Make sure this method does not block too long because
        it is called from the main worker thread."""
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        while not self._stop.wait(timeout=1):
            try:
                # Sends Heartbeat only if necessary
                self._connection.heartbeat_tick()
            except Exception as e:
                logger.error("Cannot send heartbeat: %s", e)
                self._on_error(e)
                break

            try:
                # Processes incoming heartbeats
                self._connection.drain_events(timeout=0)
            except socket.timeout:
                # No events in connection
                continue
            except Exception as e:
                logger.error("Cannot drain events from connection: %s", e)
                self._on_error(e)
                break
