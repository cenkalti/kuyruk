from __future__ import absolute_import
import errno
import select
import logging

from kuyruk.kuyruk import Kuyruk
from kuyruk.worker import Worker
from kuyruk.task import Task
from kuyruk.config import Config

__version__ = '0.17.0'

try:
    # not available in python 2.6
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

# Add NullHandler to prevent logging warnings on startup
null_handler = NullHandler()
logging.getLogger('kuyruk').addHandler(null_handler)


# Pika should do this. Patch submitted to Pika.
logging.getLogger('pika').addHandler(null_handler)


# Monkey patch ReadPoller until pika 0.9.14 is released
def ready(self):
    while True:
        try:
            return original_method(self)
            break
        except select.error as e:
            if e[0] != errno.EINTR:
                raise
from pika.adapters.blocking_connection import ReadPoller
original_method = ReadPoller.ready
ReadPoller.ready = ready
