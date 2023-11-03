import logging
import threading
from time import monotonic
from typing import Callable

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
                 user: str = 'guest',
                 password: str = 'guest',
                 vhost: str = '/',
                 connect_timeout: int = 5,
                 read_timeout: int = 5,
                 write_timeout: int = 5,
                 tcp_user_timeout: int = None,
                 heartbeat: int = 60,
                 max_idle_duration: int = None,
                 ssl: bool = False,
                 on_connect: Callable = None):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._vhost = vhost
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._tcp_user_timeout = tcp_user_timeout
        self._heartbeat = heartbeat
        self._max_idle_duration: int = max_idle_duration if max_idle_duration else heartbeat * 10
        self._ssl = ssl
        self._on_connect = on_connect

        self._connection: amqp.Connection = None
        self._lock = threading.Lock()
        self._heartbeat_thread = None
        self._stop_heartbeat = threading.Event()
        self._last_used_at: float = None  # Time of last connection used at in monotonic time

    def __enter__(self):
        self._lock.acquire()
        try:
            return self._get()
        except Exception:
            self._lock.release()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._last_used_at = monotonic()
        self._lock.release()
        return False

    def close(self):
        """Close the underlying connection."""
        with self._lock:
            if self._heartbeat_thread is not None:
                self._stop_heartbeat.set()
                self._heartbeat_thread.join()
                self._heartbeat_thread = None
                self._stop_heartbeat.clear()
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    def _get(self):
        """Returns the underlying connection if it is already connected.
        Creates a new connection if necessary."""
        if self._connection is None:
            self._connect()

        if not self._is_alive():
            self._remove()
            self._connect()

        if not self._heartbeat_thread:
            self._heartbeat_thread = self._start_heartbeat_thread()

        return self._connection

    def _connect(self):
        """Open new connection and save the object."""
        conn = self._new_connection()
        if self._on_connect:
            self._on_connect(conn)
        self._connection = conn
        return conn

    def _is_alive(self):
        """Check aliveness by sending a heartbeat frame."""
        try:
            self._connection.send_heartbeat()
        except IOError:
            return False
        else:
            return True

    def _new_connection(self):
        """Returns a new connection."""
        # Copy-pasted from kuyruk.
        socket_settings: dict[int, int] = {}
        if self._tcp_user_timeout:
            TCP_USER_TIMEOUT = 18  # constant is available on Python 3.6+.
            socket_settings[TCP_USER_TIMEOUT] = self._tcp_user_timeout * 1000

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
        logger.info("Connecting to amqp")
        conn.connect()
        return conn

    def _remove(self):
        """Close the connection and dispose connection object."""
        logger.debug("closing connection")
        try:
            self._connection.close()
        except Exception:
            pass
        self._connection = None

    def _start_heartbeat_thread(self):
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

    def _heartbeat_sender(self):
        """Target function for heartbeat thread."""
        while not self._stop_heartbeat.wait(timeout=self._heartbeat / 4):
            with self._lock:
                if self._stop_heartbeat.is_set():
                    return

                if self._connection is None:
                    continue

                if self._is_idle_enough:
                    self._remove()
                    continue

                logger.debug("sending heartbeat")
                try:
                    self._connection.send_heartbeat()
                except Exception as e:
                    logger.warning("Cannot send heartbeat: %s", e)
                    # There must be a connection error.
                    # Let's make sure that the connection is closed
                    # so next publish call can create a new connection.
                    self._remove()
