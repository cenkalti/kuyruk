import logging

from .task import Task
from .master import Master
from .config import Config

logger = logging.getLogger(__name__)


class Kuyruk(object):
    """
    Main class for Kuyruk distributed task queue. It holds the configuration
    values and provides a task decorator for user application and run method
    for workers.

    """

    def __init__(self, config_object={}):
        """
        :param config_object: See config.py for default values.

        """
        self.config = Config(config_object)
        self.before_task_functions = []
        self.after_task_functions = []

    def task(self, queue='kuyruk', eager=False, retry=0):
        """Wrap functions with this decorator to convert them to background
        tasks. After wrapping, normal calls will send a message to queue
        instead of running the actual function.

        :param queue: Queue name for the tasks
        :param eager: Run task in process, do not use RabbitMQ
        :param retry: Retry this times before give up
        :return: Callable Task object wrapping the original function
        """
        def decorator():
            def inner(f):
                queue_ = 'kuyruk' if callable(queue) else queue
                return Task(f, self, queue=queue_, eager=eager, retry=retry)
            return inner

        if callable(queue):
            logger.debug('task without args')
            return decorator()(queue)
        else:
            logger.debug('task with args')
            return decorator()

    def run(self, queues=None):
        """Run Kuyruk workers.
        :param queues: queues str passed directly from command line
        :return: None

        This function may exit() before returning if SIGINT or SIGTERM
        received.

        """
        Master(self.config).run(queues)

    def before_task(self, f):
        self.before_task_functions.append(f)
        return f

    def after_task(self, f):
        self.after_task_functions.append(f)
        return f
