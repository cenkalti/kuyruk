from __future__ import absolute_import
import logging

from kuyruk.task import Task
from kuyruk.master import Master
from kuyruk.config import Config
from kuyruk.eventmixin import EventMixin

logger = logging.getLogger(__name__)


class Kuyruk(EventMixin):
    """
    Main class for Kuyruk distributed task queue. It holds the configuration
    values and provides a task decorator for user application and run method
    for workers.

    """

    def __init__(self, config=None):
        """
        :param config: See config.py for default values.

        """
        super(Kuyruk, self).__init__()
        self.config = Config()
        if config:
            self.config.from_object(config)

    def task(self, queue='kuyruk', eager=False, retry=0, task_class=None):
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
                task_class_ = task_class or Task
                return task_class_(
                    f, self, queue=queue_, eager=eager, retry=retry)
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
        master = Master(self.config)
        master.override_queues = queues
        master.run()

    class Reject(Exception):
        """
        The task should raise this if it does not want to process the message.
        In this case message will be requeued and delivered to another worker.

        """
        pass
