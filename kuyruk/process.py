import os
import sys
import signal
import logging
import logging.config
import threading
import traceback
from time import time
from .config import Config
from .manager.client import ManagerClientThread

logger = logging.getLogger(__name__)


class KuyrukProcess(object):
    """Base class for Master and Worker classes.
    Contains some shared code betwee these 2 classes.

    """
    def __init__(self, config):
        assert isinstance(config, Config)
        self.config = config
        self.shutdown_pending = threading.Event()
        self.manager_thread = None
        self.popen = None

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
            self.warm_shutdown(signum == signal.SIGINT)
        else:
            self.cold_shutdown()
        logger.debug("Handled SIGINT")

    def warm_shutdown(self, sigint):
        logger.warning("Warm shutdown")
        self.shutdown_pending.set()

    def cold_shutdown(self):
        logger.warning("Cold shutdown")
        self.shutdown_pending.set()
        os._exit(0)

    def maybe_start_manager_thread(self):
        if self.config.MANAGER_HOST:
            self.manager_thread = ManagerClientThread(
                self.config.MANAGER_HOST,
                self.config.MANAGER_PORT,
                self.get_stats,
                self.on_action)
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
            logging.basicConfig(level=level)


def print_stack(sig, frame):
    print '=' * 70
    print ''.join(traceback.format_stack())
    print '-' * 70
