import logging
import importlib.metadata

from kuyruk.kuyruk import Kuyruk
from kuyruk.task import Task
from kuyruk.config import Config
from kuyruk.worker import Worker

__all__ = ['Kuyruk', 'Config', 'Task', 'Worker']

__version__ = importlib.metadata.version('kuyruk')

logger = logging.getLogger(__name__)

# Add NullHandler to prevent logging warnings on startup
null_handler = logging.NullHandler()
logger.addHandler(null_handler)
