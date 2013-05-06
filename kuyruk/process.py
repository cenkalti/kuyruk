import os
import signal
import logging
import threading
import multiprocessing
from time import time
from .manager.client import ManagerClientThread

logger = logging.getLogger(__name__)


class Process(multiprocessing.Process):

    def __init__(self, config):
        super(Process, self).__init__()
        self.config = config
        self.manager_thread = None
        self.shutdown_pending = threading.Event()

    def run(self):
        self.register_signals()
        logger.debug('PID: %s PGID: %s', os.getpid(), os.getpgrp())
        self.started = time()

    def register_signals(self):
        raise NotImplementedError

    def maybe_start_manager_thread(self):
        if self.config.MANAGER_HOST:
            self.start_manager_thread()

    def start_manager_thread(self):
        self.manager_thread = ManagerClientThread(
            self.config.MANAGER_HOST,
            self.config.MANAGER_PORT,
            self.get_stats,
            self.on_action)
        self.manager_thread.start()

    def get_stats(self):
        raise NotImplementedError

    def on_action(self, sock, action):
        """Run the function sent by the manager."""
        f, args, kwargs = action
        logger.info(f, args, kwargs)
        f = getattr(self, f)
        f(*args, **kwargs)

    @property
    def uptime(self):
        return int(time() - self.started)

    def kill_pg(self):
        try:
            os.killpg(self.pid, signal.SIGKILL)
        except OSError as e:
            if e.errno != 3:  # No such process
                raise
