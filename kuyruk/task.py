import logging
from datetime import datetime
from functools import wraps

from .queue import Queue
from . import loader
from .connection import LazyConnection

logger = logging.getLogger(__name__)


class TaskResult(object):

    def __init__(self, task):
        self.task = task

    def __getattr__(self, item):
        raise Exception(item)

    def __getitem__(self, item):
        raise Exception(item)

    def __setitem__(self, key, value):
        raise Exception(key, value)

    def __repr__(self):
        return '<TaskResult of %s>' % self.task.fully_qualified_name

    def __str__(self):
        return self.__repr__()


class Task(object):

    def __init__(self, f, kuyruk, queue='kuyruk',
                 local=False, eager=False, retry=0):
        self.f = f
        self.kuyruk = kuyruk
        self.queue = queue
        self.local = local
        self.eager = eager
        self.retry = retry
        self.before_task_function = None
        self.after_task_function = None

    def __repr__(self):
        return "<Task %s>" % self.fully_qualified_name

    def __call__(self, *args, **kwargs):
        fname = self.fully_qualified_name
        assert self.is_reachable(fname, self.f)
        logger.debug('fname: %s', fname)
        if self.kuyruk.config.EAGER or self.eager:
            self.run(args, kwargs)
        else:
            self.send_to_queue(args, kwargs)

        return TaskResult(self)

    def send_to_queue(self, args=(), kwargs={}):
        """
        Send this task to queue

        :param args: Arguments that will be passed to task on execution
        :param kwargs: Keyword arguments that will be passed to task on execution
        :return: None

        """
        task_description = {
            'f': self.fully_qualified_name,
            'args': args, 'kwargs': kwargs,
            'timestamp': str(datetime.utcnow())
        }
        if self.retry:
            task_description['retry'] = self.retry

        connection = LazyConnection(
            self.kuyruk.config.RABBIT_HOST, self.kuyruk.config.RABBIT_PORT,
            self.kuyruk.config.RABBIT_USER, self.kuyruk.config.RABBIT_PASSWORD)
        channel = connection.channel()
        with connection:
            with channel:
                queue = Queue(self.queue, channel, self.local)
                queue.send(task_description)

    def run(self, args, kwargs):
        """Run the task function with before and after task functions."""
        if self.before_task_function:
            self.before_task_function()

        # Run wrapped function
        self.f(*args, **kwargs)

        if self.after_task_function:
            self.after_task_function()

    @property
    def fully_qualified_name(self):
        return loader.get_fully_qualified_function_name(self.f)

    def is_reachable(self, fname, f):
        imported = loader.import_task(fname)
        return imported.f is f

    def before_task(self, f):
        self.before_task_function = f
        return f

    def after_task(self, f):
        self.after_task_function = f
        return f
