import socket
import logging
import threading
from time import monotonic
from types import TracebackType
from typing import Literal, Optional, Type, Dict

import amqp
import amqp.exceptions

logger = logging.getLogger(__name__)


class SingleConnection:
    """SingleConnection is a helper for dealing with amqp.Connection objects
    from multiple threads.

    Use it as a context manager to get the underlying connection.
    In the context the connection is locked so no other thread can use
    the connection. Make sure you don't hold the lock longer than needed.

    """

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 5672,
                 vhost: str = '/',
                 user: str = 'guest',
                 password: str = 'guest',
                 ssl: bool = False,
                 heartbeat: int = 60,
                 max_idle_duration: int = 60,
                 connect_timeout: int = 5,
                 read_timeout: int = 5,
                 write_timeout: int = 5,
                 tcp_user_timeout: int = 60):
        self._host = host
        self._port = port
        self._vhost = vhost
        self._user = user
        self._password = password
        self._ssl = ssl
        self._heartbeat = heartbeat
        self._max_idle_duration = max_idle_duration
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._tcp_user_timeout = tcp_user_timeout

        self._connection: amqp.Connection = None
        self._lock = threading.RLock()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_heartbeat = threading.Event()
        self._last_used_at: float = 0  # Time of last connection used at in monotonic time

    def __enter__(self) -> amqp.Connection:
        """Acquire the lock and return underlying connection."""
        self._lock.acquire()
        try:
            return self._get()
        except Exception:
            self._lock.release()
            raise

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> Literal[False]:
        """Release the lock."""
        self._last_used_at = monotonic()
        self._lock.release()
        return False  # False re-raises the exception.

    # Called by GC on cleanup.
    def __del__(self) -> None:
        self.close(suppress_exceptions=True)

    def close(self, suppress_exceptions: bool = False) -> None:
        """Close the underlying connection."""
        with self._lock:
            self._remove_heartbeat_thread()
            self._remove_connection(suppress_exceptions=suppress_exceptions)

    def _get(self) -> amqp.Connection:
        """Returns the underlying connection if it is already connected.
        Creates a new connection if necessary."""
        if self._connection is None:
            self._connection = self.new_connection()

        if not self._is_alive:
            logger.error("RabbitMQ connection is not alive. New connection will be opened.")
            self._remove_connection(suppress_exceptions=True)
            self._connection = self.new_connection()

        if not self._heartbeat_thread:
            self._heartbeat_thread = self._start_heartbeat_thread()

        return self._connection

    @property
    def _is_alive(self) -> bool:
        """Check aliveness by sending a heartbeat frame."""
        try:
            self._connection.send_heartbeat()
        except Exception:
            return False

        try:
            self._connection.drain_events(timeout=0)
        except socket.timeout:
            return True
        except Exception:
            return False
        else:
            return True

    def new_connection(self) -> amqp.Connection:
        """Returns a new connection."""
        socket_settings: Dict[int, int] = {}
        if self._tcp_user_timeout and hasattr(socket, 'TCP_USER_TIMEOUT'):
            socket_settings[socket.TCP_USER_TIMEOUT] = self._tcp_user_timeout * 1000

        conn = amqp.Connection(
            host="{h}:{p}".format(h=self._host, p=self._port),
            userid=self._user,
            password=self._password,
            virtual_host=self._vhost,
            connect_timeout=self._connect_timeout,
            read_timeout=self._read_timeout,
            write_timeout=self._write_timeout,
            socket_settings=socket_settings,
            heartbeat=self._heartbeat,
            ssl=self._ssl)
        conn.connect()
        logger.info('Connected to RabbitMQ')
        return conn

    def _remove_connection(self, suppress_exceptions: bool) -> None:
        """Close the connection and dispose connection object."""
        if self._connection is not None:
            logger.debug("Closing RabbitMQ connection")
            try:
                self._connection.close()
            except Exception:
                if not suppress_exceptions:
                    raise
            else:
                self._connection = None

    def _remove_heartbeat_thread(self) -> None:
        if self._heartbeat_thread is not None:
            self._stop_heartbeat.set()
            self._heartbeat_thread.join()
            self._heartbeat_thread = None
            self._stop_heartbeat.clear()

    def _start_heartbeat_thread(self) -> threading.Thread:
        t = threading.Thread(target=self._heartbeat_sender)
        t.daemon = True
        t.start()
        return t

    @property
    def _is_idle_enough(self) -> bool:
        if not self._last_used_at:
            return False

        delta = monotonic() - self._last_used_at
        return delta > self._max_idle_duration

    def _heartbeat_sender(self) -> None:
        """Target function for heartbeat thread."""
        while not self._stop_heartbeat.wait(timeout=self._heartbeat / 4):
            with self._lock:
                if self._stop_heartbeat.is_set():
                    return

                if self._connection is None:
                    continue

                if self._is_idle_enough:
                    logger.debug("Removing idle connection")
                    self._remove_connection(suppress_exceptions=True)
                    continue

                logger.debug("Sending heartbeat")
                try:
                    self._connection.send_heartbeat()
                except Exception as e:
                    logger.error("Cannot send heartbeat: %s", e)
                    # There must be a connection error.
                    # Let's make sure that the connection is closed
                    # so next publish call can create a new connection.
                    self._remove_connection(suppress_exceptions=True)

                try:
                    self._connection.drain_events(timeout=0)
                except socket.timeout:
                    # No events in connection
                    pass
                except Exception as e:
                    logger.error("Cannot drain events from connection: %s", e)
                    self._remove_connection(suppress_exceptions=True)
