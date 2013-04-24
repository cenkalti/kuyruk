import logging

from .kuyruk import Kuyruk
from .task import Task
from .worker import Worker
from .queue import Queue
from .exceptions import Reject

__version__ = '0.4.0'


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
