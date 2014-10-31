from __future__ import absolute_import
import os
import sys
import signal
import logging
import logging.config
import threading
from time import time

from kuyruk.helpers import print_stack


logger = logging.getLogger(__name__)


class KuyrukProcess(object):
    """Base class for Master and Worker classes.
    Contains some shared code between these 2 classes.

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
        self.setup_logging()
        self.register_signals()
        logger.debug('PID: %s PGID: %s', os.getpid(), os.getpgrp())
        self.started = time()

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

    @staticmethod
    def _exit(status=0):
        """Flush output buffers and exit from process."""
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(status)

    def rpc_service_class(self):
        raise NotImplementedError

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
            logging.getLogger('rabbitpy').level = logging.WARNING
            level = getattr(logging, self.config.LOGGING_LEVEL.upper())
            fmt = "%(levelname).1s %(process)d " \
                  "%(name)s.%(funcName)s:%(lineno)d - %(message)s"
            logging.basicConfig(level=level, format=fmt)
