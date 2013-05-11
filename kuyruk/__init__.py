from __future__ import absolute_import
import logging

from kuyruk.kuyruk import Kuyruk
from kuyruk.task import Task
from kuyruk.version import __version__

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
