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

# Add NullHandler to prevent logging warnings on startup
logging.getLogger(__name__).addHandler(NullHandler())

# TODO use exit codes from child workers to indicate success or fail
# TODO class tasks (resolver)
# TODO set kuyruk log level
# TODO set application log level
# TODO write a script for requeueing failed tasks
# TODO coverage
# TODO model extension
# TODO support python 3
