from __future__ import absolute_import
import os
import sys
import signal
import socket
import logging
from time import time
from uuid import uuid1
from datetime import datetime
from functools import wraps
from contextlib import contextmanager

from kuyruk import events, importer
from kuyruk.queue import Queue
from kuyruk.events import EventMixin
from kuyruk.exceptions import Timeout, InvalidTask, ObjectNotFound

logger = logging.getLogger(__name__)


def profile(f):
    """Logs the time spent while running the task."""
    @wraps(f)
    def inner(self, *args, **kwargs):
        start = time()
        result = f(self, *args, **kwargs)
        end = time()
        logger.info("%s finished in %i seconds." % (self.name, end - start))
        return result
    return inner


def object_to_id(f):
    """
    If the Task is a class task, converts the first parameter to the id.

    """
    @wraps(f)
    def inner(self, *args, **kwargs):
        cls = self.cls or self.arg_class
        if cls:
            try:
                obj = args[0]
            except IndexError:
                msg = "You must give an instance of %s as first argument." % cls
                raise TypeError(msg)

            if not isinstance(obj, cls):
                msg = "First argument must be an instance of %s." % cls
                raise TypeError(msg)

            args = list(args)
            args[0] = args[0].id
            assert isinstance(args[0], (int, long, basestring))
        return f(self, *args, **kwargs)
    return inner


def id_to_object(f):
    """
    If the Task is a class task, converts the first argument to an object
    by calling the get function of the class with the id.

    """
    @wraps(f)
    def inner(self, *args, **kwargs):
        cls = self.arg_class or self.cls
        if cls:
            if not args:
                raise InvalidTask

            obj_id = args[0]
            if not isinstance(obj_id, (int, long, basestring)):
                raise InvalidTask

            obj = cls.get(obj_id)
            if obj is None:
                raise ObjectNotFound

            if not isinstance(obj, cls):
                msg = "%s is not an instance of %s." % (obj, cls)
                raise ObjectNotFound(msg)

            args = list(args)
            args[0] = obj

        return f(self, *args, **kwargs)
    return inner


def send_client_signals(f):
    """Sends the presend and postsend signals."""
    @wraps(f)
    def inner(self, *args, **kwargs):
        self.send_signal(events.task_presend, args, kwargs, reverse=True)
        rv = f(self, *args, **kwargs)
        self.send_signal(events.task_postsend, args, kwargs)
        return rv
    return inner


class Task(EventMixin):

    def __init__(self, f, kuyruk, queue='kuyruk', local=False, eager=False,
                 retry=0, max_run_time=None, arg_class=None):
        self.f = f
        self.kuyruk = kuyruk
        self.queue_name = queue
        self.local = local
        self.eager = eager
        self.retry = retry
        self.max_run_time = max_run_time
        self.cls = None
        self.arg_class = arg_class
        self.setup()

    def setup(self):
        """Convenience function for extending classes
        that run after __init__."""
        pass

    def __repr__(self):
        return "<Task of %r>" % self.name

    @send_client_signals
    @object_to_id
    def __call__(self, *args, **kwargs):
        """When a fucntion is wrapped with a task decorator it will be
        converted to a Task object. By overriding __call__ method we are
        sending this task to queue instead of invoking the function
        without changing the client code.

        """
        logger.debug("Task.__call__ args=%r, kwargs=%r", args, kwargs)

        # These keyword argument allow the sender to override
        # the destination of the message.
        host = kwargs.pop('kuyruk_host', None)
        local = kwargs.pop('kuyruk_local', None)
        expiration = kwargs.pop('kuyruk_expiration', None)

        if self.eager or self.kuyruk.config.EAGER:
            # Run the task in process
            task_result = self._run(*args, **kwargs)
        else:
            # Send it to the queue
            task_result = TaskResult(self)
            task_result.id = self.send_to_queue(args, kwargs,
                                                host=host, local=local,
                                                expiration=expiration)

        return task_result

    def delay(self, *args, **kwargs):
        """Compatibility function for migrating existing Celery project to
        Kuyruk."""
        self.__call__(*args, **kwargs)

    def __get__(self, obj, objtype):
        """If the task is accessed from an instance via attribute syntax
        returns a bound task object that wraps the task itself, otherwise
        returns the task itself.

        This is done for allowing a method to be converted to task without
        modifying the client code. When a function decorated inside a class
        there is no way of accessing that class at that time because methods
        are bounded at run time when they are accessed. The trick here is that
        we set self.cls when the Task is accessed first time via attribute
        syntax.

        """
        self.cls = objtype
        if obj:
            # Class tasks needs to know what the object is so they can
            # inject that object in front of args.
            # We are returning a BoundTask instance here wrapping this task
            # that will do the injection.
            logger.debug("Creating bound task with obj=%r", obj)
            return BoundTask(self, obj)
        return self

    def send_to_queue(self, args, kwargs, host=None, local=None,
                      expiration=None):
        """
        Sends this task to queue.

        :param args: Arguments that will be passed to task on execution.
        :param kwargs: Keyword arguments that will be passed to task
            on execution.
        :param host: Send this task to specific host. ``host`` will be
            appended to the queue name.
        :param local: Send this task to this host. Hostname of this host will
            be appended to the queue name.
        :param expiration: Expire message after expiration milliseconds.
        :return: :const:`None`

        """
        logger.debug("Task.send_to_queueue args=%r, kwargs=%r", args, kwargs)

        with self.queue(host=host, local=local) as queue:
            desc = self.get_task_description(args, kwargs, queue.name)
            queue.send(desc, expiration=expiration)

        # We are returning the unique identifier of the task sent to queue
        # so we can query the result backend for completion.
        # TODO no result backend is available yet
        return desc['id']

    @contextmanager
    def queue(self, host=None, local=None):
        queue_ = self.queue_name
        local_ = self.local

        if local is not None:
            local_ = local

        if host:
            queue_ = "%s.%s" % (self.queue_name, host)
            local_ = False

        yield Queue(queue_, self.kuyruk.channel(), local_)

    def get_task_description(self, args, kwargs, queue):
        """Return the dictionary to be sent to the queue."""
        return {
            'id': uuid1().hex,
            'queue': queue,
            'args': args,
            'kwargs': kwargs,
            'module': self.module_name,
            'function': self.f.__name__,
            'class': self.class_name,
            'retry': self.retry,
            'sender_timestamp': datetime.utcnow(),
            'sender_hostname': socket.gethostname(),
            'sender_pid': os.getpid(),
            'sender_cmd': ' '.join(sys.argv),
        }

    def send_signal(self, sig, args, kwargs, reverse=False, **extra):
        """
        Sends a signal for each sender.
        This allows the user to register for a specific sender.

        """
        senders = (self, self.__class__, self.kuyruk)
        if reverse:
            senders = reversed(senders)

        for sender in senders:
            sig.send(sender, task=self, args=args, kwargs=kwargs, **extra)

    @send_client_signals
    @object_to_id
    def apply(self, *args, **kwargs):
        logger.debug("Task.apply args=%r, kwargs=%r", args, kwargs)
        return self._run(*args, **kwargs)

    @profile
    @id_to_object
    def _run(self, *args, **kwargs):
        """Run the wrapped function and event handlers."""
        def send_signal(sig, reverse=False, **extra):
            self.send_signal(sig, args, kwargs, reverse, **extra)

        logger.debug("Task._apply args=%r, kwargs=%r", args, kwargs)

        result = TaskResult(self)

        limit = (self.max_run_time or
                 self.kuyruk.config.MAX_TASK_RUN_TIME or 0)

        logger.debug("Applying %r, args=%r, kwargs=%r", self, args, kwargs)
        try:
            send_signal(events.task_prerun, reverse=True)
            with time_limit(limit):
                return_value = self.run(*args, **kwargs)
        except Exception:
            send_signal(events.task_failure, exc_info=sys.exc_info())
            raise
        else:
            send_signal(events.task_success, return_value=return_value)
            result.result = return_value
        finally:
            send_signal(events.task_postrun)

        # We are returning a TaskResult here because __call__ returns a
        # TaskResult object too. Return value must be consistent whether
        # task is sent to queue or executed in process with apply().
        return result

    def run(self, *args, **kwargs):
        """
        Runs the wrapped function.
        This method is intended to be overriden from subclasses.

        """
        return self.f(*args, **kwargs)

    @property
    def name(self):
        """Location for the wrapped function.
        This value is by the worker to find the task.

        """
        if self.class_name:
            return "%s:%s.%s" % (
                self.module_name, self.class_name, self.f.__name__)
        else:
            return "%s:%s" % (self.module_name, self.f.__name__)

    @property
    def module_name(self):
        """Module name of the function wrapped."""
        name = self.f.__module__
        if name == '__main__':
            name = importer.get_main_module().name
        return name

    @property
    def class_name(self):
        """Name of the class if this is a class task,
        otherwise :const:`None`."""
        if self.cls:
            return self.cls.__name__


class BoundTask(Task):
    """
    This class wraps the Task and inject the bound object in front of args
    when it is called.

    """
    def __init__(self, task, obj):
        self.task = task
        self.obj = obj

    def __getattr__(self, item):
        """Delegates all attributes to real Task."""
        return getattr(self.task, item)

    @wraps(Task.__call__)
    def __call__(self, *args, **kwargs):
        logger.debug("BoundTask.__call__ args=%r, kwargs=%r", args, kwargs)
        # Insert the bound object as a first argument to __call__
        args = list(args)
        args.insert(0, self.obj)
        return super(BoundTask, self).__call__(*args, **kwargs)

    @wraps(Task.apply)
    def apply(self, *args, **kwargs):
        logger.debug("BoundTask.apply args=%r, kwargs=%r", args, kwargs)
        args = list(args)
        args.insert(0, self.obj)
        return super(BoundTask, self).apply(*args, **kwargs)


class TaskResult(object):
    """Insance of this class is returned after the task is sent to queue.
    Since Kuyruk does not support a result backend yet it will raise
    exception on any attribute or item access.

    """
    def __init__(self, task):
        self.task = task

    def __getattr__(self, name):
        raise NotImplementedError(name)

    def __setattr__(self, name, value):
        if name not in ('task', 'id', 'result'):
            raise NotImplementedError(name)
        super(TaskResult, self).__setattr__(name, value)

    def __getitem__(self, key):
        raise NotImplementedError(key)

    def __setitem__(self, key, value):
        raise NotImplementedError(key)

    def __repr__(self):
        return "<TaskResult of %r>" % self.task.name


@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise Timeout
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
