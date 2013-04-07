import logging
try:
    # not available in python 2.6
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

from .kuyruk import Kuyruk
from .task import Task
from .worker import Worker
from .queue import Queue
from .exceptions import Reject

logging.getLogger(__name__).addHandler(NullHandler())

# TODO set kuyruk log level
# TODO set application log level
# TODO requeue failed tasks
# TODO coverage
# TODO test disconnect and reconnect
# TODO model extension
