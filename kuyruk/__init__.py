from __future__ import absolute_import
import logging

from kuyruk.kuyruk import Kuyruk
from kuyruk.worker import Worker
from kuyruk.task import Task
from kuyruk.config import Config

__version__ = '0.13.4'

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
logging.getLogger('pika').addHandler(null_handler)
