import logging
from time import time
from types import MethodType
from datetime import datetime

from . import loader
from .queue import Queue
from .connection import LazyConnection

logger = logging.getLogger(__name__)


def profile(f):
    def inner(self, *args, **kwargs):
        start = time()
        result = f(self, *args, **kwargs)
        end = time()
        logger.info("%r finished in %i seconds." % (self, end - start))
        return result
    return inner


class Task(object):

    def __init__(self, f, kuyruk, queue='kuyruk',
                 local=False, eager=False, retry=0):
        self.f = f
        self.kuyruk = kuyruk
        self.queue = queue
        self.local = local
        self.eager = eager
        self.retry = retry
        self.cls = None
        self.before_task_functions = []
        self.after_task_functions = []

    def __repr__(self):
        return "<Task of %r>" % self.name

    def __call__(self, *args, **kwargs):
        if self.eager or self.kuyruk.config.EAGER:
            self.run(args, kwargs)
        else:
            self.send_to_queue(args, kwargs)

        return TaskResult(self)

    def __get__(self, obj, objtype):
        if obj:
            self.cls = objtype
            return MethodType(self.__call__, obj, objtype)
        return self

    def send_to_queue(self, args=(), kwargs={}):
        """
        Send this task to queue

        :param args: Arguments that will be passed to task on execution
        :param kwargs: Keyword arguments that will be passed to task on execution
        :return: None

        """
        task_description = {
            'module': self.module_name,
            'function': self.f.__name__,
            'class': self.class_name,
            'object_id': args[0].id if self.cls else None,
            'args': args[1:] if self.cls else args,
            'kwargs': kwargs,
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

    @profile
    def run(self, args, kwargs):
        """Run the wrapped function with before and after task functions."""
        def run(functions):
            [f() for f in functions]

        run(self.kuyruk.before_task_functions)
        run(self.before_task_functions)
        self.call_wrapped(args, kwargs)
        run(self.after_task_functions)
        run(self.kuyruk.after_task_functions)

    def call_wrapped(self, args, kwargs):
        if self.cls:
            args = list(args)
            obj = self.cls.get(args[0])
            args.insert(0, obj)

        self.f(*args, **kwargs)

    @property
    def name(self):
        if self.class_name:
            return '.'.join([self.module_name, self.class_name, self.f.__name__])
        else:
            return '.'.join([self.module_name, self.f.__name__])

    @property
    def module_name(self):
        name = self.f.__module__
        if name == '__main__':
            name = loader.get_main_module().name
        return name

    @property
    def class_name(self):
        if self.cls:
            return self.cls.__name__

    def before_task(self, f):
        self.before_task_functions.append(f)
        return f

    def after_task(self, f):
        self.after_task_functions.append(f)
        return f


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
        return "<TaskResult of %r>" % self.task.name

    def __str__(self):
        return self.__repr__()
