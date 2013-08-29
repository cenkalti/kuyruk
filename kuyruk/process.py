from __future__ import absolute_import
import os
import sys
import signal
import logging
import logging.config
import threading
import subprocess
from time import time

from kuyruk.helpers import monkeypatch_method, print_stack
from kuyruk.manager.client import ManagerClientThread


logger = logging.getLogger(__name__)


class KuyrukProcess(object):
    """Base class for Master and Worker classes.
    Contains some shared code betwee these 2 classes.

    """
    def __init__(self, kuyruk):
        from kuyruk import Kuyruk
        assert isinstance(kuyruk, Kuyruk)
        self.kuyruk = kuyruk
        self.shutdown_pending = threading.Event()
        self.manager_thread = None
        self.popen = None

    @property
    def config(self):
        return self.kuyruk.config

    def run(self):
        if self.config.CLOSE_FDS is True:
            self.patch_popen()

        self.setup_logging()
        self.register_signals()
        logger.debug('PID: %s PGID: %s', os.getpid(), os.getpgrp())
        self.started = time()

    def patch_popen(self):
        """Patch subprocess.Popen constructor to close_fds by default."""
        original_init = subprocess.Popen.__init__

        @monkeypatch_method(subprocess.Popen)
        def __init__(self, *args, **kwargs):
            close_fds = kwargs.pop('close_fds', True)
            original_init(self, *args, close_fds=close_fds, **kwargs)

    def register_signals(self):
        signal.signal(signal.SIGINT, self.handle_sigint)
        signal.signal(signal.SIGUSR1, print_stack)  # for debugging

    def handle_sigint(self, signum, frame):
        """If running from terminal pressing Ctrl-C will initiate a warm
        shutdown. The second interrupt will do a cold shutdown.

        """
        logger.warning("Handling SIGINT")
        if sys.stdin.isatty() and not self.shutdown_pending.is_set():
            self.warm_shutdown()
        else:
            self.cold_shutdown()
        logger.debug("Handled SIGINT")

    def warm_shutdown(self):
        logger.warning("Warm shutdown")
        self.shutdown_pending.set()

    def cold_shutdown(self):
        logger.warning("Cold shutdown")
        sys.exit(0)

    def maybe_start_manager_thread(self, socket_lock=None):
        if self.config.MANAGER_HOST:
            self.manager_thread = ManagerClientThread(
                self.config.MANAGER_HOST,
                self.config.MANAGER_PORT,
                self.get_stats,
                self.on_action,
                socket_lock=socket_lock)
            self.manager_thread.start()

    def get_stats(self):
        """Returns the stats for sending to Kuyruk Manager.
        Called by the manager thread. (self.manager_thread)"""
        raise NotImplementedError

    def on_action(self, sock, action):
        """Run the function sent by the manager."""
        f, args, kwargs = action
        logger.info("action: f=%s, args=%s, kwargs=%s", f, args, kwargs)
        f = getattr(self, f)
        f(*args, **kwargs)

    @property
    def uptime(self):
        return int(time() - self.started)

    def setup_logging(self):
        if self.config.LOGGING_CONFIG:
            logging.config.fileConfig(self.config.LOGGING_CONFIG)
        else:
            logging.getLogger('pika').level = logging.WARNING
            level = getattr(logging, self.config.LOGGING_LEVEL.upper())
            fmt = "%(levelname).1s %(process)d " \
                  "%(name)s.%(funcName)s:%(lineno)d - %(message)s"
            logging.basicConfig(level=level, format=fmt)
