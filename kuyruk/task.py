from __future__ import absolute_import
import sys
import logging
from types import MethodType
from datetime import datetime

from kuyruk import signals, importer
from kuyruk.queue import Queue
from kuyruk.helpers import profile
from kuyruk.channel import LazyChannel
from kuyruk.eventmixin import EventMixin

logger = logging.getLogger(__name__)


class Task(EventMixin):

    def __init__(self, f, kuyruk, queue='kuyruk',
                 local=False, eager=False, retry=0):
        self.f = f
        self.kuyruk = kuyruk
        self.queue = queue
        self.local = local
        self.eager = eager
        self.retry = retry
        self.cls = None
        self.setup()

    def setup(self):
        pass

    def __repr__(self):
        return "<Task of %r>" % self.name

    def __call__(self, *args, **kwargs):
        if self.eager or self.kuyruk.config.EAGER:
            self.apply(args, kwargs)
        else:
            self.send_to_queue(args, kwargs)

        return TaskResult(self)

    def __get__(self, obj, objtype):
        """If the task is accessed from an instance via attribute syntax
        return a function for sending the task to queue, otherwise
        return the task itself.

        This is done for allowing a method to be converted to task without
        modifying the client code. When a function decorated inside a class
        there is no way of accessing that class at that time because methods
        are bounded at run time when they are accessed. The trick here is that
        we set self.cls when the Task is accessed first time via attribute
        syntax.

        """
        self.cls = objtype
        if obj:
            return MethodType(self.__call__, obj, objtype)
        return self

    def send_to_queue(self, args, kwargs):
        """
        Send this task to queue

        :param args: Arguments that will be passed to task on execution
        :param kwargs: Keyword arguments that will be passed to task on execution
        :return: None

        """
        desc = self.get_task_description(args, kwargs)
        channel = LazyChannel.from_config(self.kuyruk.config)
        with channel:
            queue = Queue(self.queue, channel, self.local)
            queue.send(desc)

    def get_task_description(self, args, kwargs):
        # For class tasks; replace the first argument with the id of the object
        if self.cls:
            args = list(args)
            args[0] = args[0].id

        return {
            'module': self.module_name,
            'function': self.f.__name__,
            'class': self.class_name,
            'args': args,
            'kwargs': kwargs,
            'timestamp': str(datetime.utcnow()),
            'retry': self.retry,
        }

    @profile
    def apply(self, args, kwargs):
        """Run the wrapped function and event handlers."""
        SENDERS = (self, self.__class__, self.kuyruk)

        def send_signal(signal, senders, **extra):
            for sender in senders:
                signal.send(
                    sender, task=self, args=args, kwargs=kwargs, **extra)

        try:
            send_signal(signals.before_task, reversed(SENDERS))
            return_value = self.f(*args, **kwargs)  # call wrapped function
        except Exception:
            send_signal(
                signals.on_exception, SENDERS, exc_info=sys.exc_info())
            raise
        else:
            send_signal(
                signals.on_return, SENDERS, return_value=return_value)
        finally:
            send_signal(signals.after_task, SENDERS)

    @property
    def name(self):
        if self.class_name:
            return "%s:%s.%s" % (self.module_name, self.class_name,
                                 self.f.__name__)
        else:
            return "%s:%s" % (self.module_name, self.f.__name__)

    @property
    def module_name(self):
        name = self.f.__module__
        if name == '__main__':
            name = importer.get_main_module().name
        return name

    @property
    def class_name(self):
        if self.cls:
            return self.cls.__name__


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
