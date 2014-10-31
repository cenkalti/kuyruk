from __future__ import absolute_import
import os
import sys
import signal
import socket
import logging
from time import time
from uuid import uuid1
from functools import wraps
from threading import Lock
from contextlib import contextmanager

import rabbitpy
import rabbitpy.exceptions

from kuyruk import events, importer
from kuyruk.events import EventMixin
from kuyruk.exceptions import Timeout, InvalidTask, ObjectNotFound

logger = logging.getLogger(__name__)

_DECLARE_ALWAYS = False


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
        self._send_signal(events.task_presend, args, kwargs, reverse=True)
        rv = f(self, *args, **kwargs)
        self._send_signal(events.task_postsend, args, kwargs)
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
        self._declared_queues = set()
        self._declared_queues_lock = Lock()
        self.setup()

    def setup(self):
        """Override from subclass to initialize the task.
        This function is called only once when the task is created."""
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

        # Allow the sender to override the destination of the message.
        host = kwargs.pop('kuyruk_host', None)

        if self.eager or self.kuyruk.config.EAGER:
            # Run the task in process
            self._run(*args, **kwargs)
        else:
            # Send it to the queue
            self._send_to_queue(args, kwargs, host=host)

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

    def _declare_queue(self, channel, name, force=False):
        with self._declared_queues_lock:
            if name not in self._declared_queues or force or _DECLARE_ALWAYS:
                logger.debug("declaring queue...")
                rabbitpy.Queue(channel, name=name, durable=True).declare()
                self._declared_queues.add(name)

    def _send_to_queue(self, args, kwargs, host=None):
        """
        Sends this task to queue.

        :param args: Arguments that will be passed to task on execution.
        :param kwargs: Keyword arguments that will be passed to task
            on execution.
        :param host: Send this task to specific host. ``host`` will be
            appended to the queue name.
        :return: :const:`None`

        """
        logger.debug("Task.send_to_queueue args=%r, kwargs=%r", args, kwargs)

        if host:
            queue_name = ".".join((self.queue_name, host))
        elif self.local:
            queue_name = ".".join((self.queue_name, socket.gethostname()))
        else:
            queue_name = self.queue_name

        desc = self._get_task_description(args, kwargs, queue_name)
        properties = {"delivery_mode": 2, "content_type": "application/json"}

        ch = self.kuyruk._channel_for_publishing
        self._declare_queue(ch, queue_name)
        logger.debug("publishing message...")
        msg = rabbitpy.Message(ch, desc, properties=properties)
        try:
            msg.publish(exchange="", routing_key=queue_name, mandatory=True)
        except rabbitpy.exceptions.MessageReturnedException:
            self._declare_queue(ch, queue_name, force=True)
            msg.publish(exchange="", routing_key=queue_name, mandatory=True)
        logger.debug("published.")

    def _get_task_description(self, args, kwargs, queue):
        """Return the dictionary to be sent to the queue."""
        return {
            'id': uuid1().hex,
            'queue': queue,
            'args': args,
            'kwargs': kwargs,
            'module': self._module_name,
            'function': self.f.__name__,
            'class': self._class_name,
            'retry': self.retry,
            'sender_hostname': socket.gethostname(),
            'sender_pid': os.getpid(),
            'sender_cmd': ' '.join(sys.argv),
        }

    def _send_signal(self, sig, args, kwargs, reverse=False, **extra):
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
            self._send_signal(sig, args, kwargs, reverse, **extra)

        logger.debug("Task._apply args=%r, kwargs=%r", args, kwargs)

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
        finally:
            send_signal(events.task_postrun)

    def run(self, *args, **kwargs):
        """
        Runs the wrapped function.
        This method is intended to be overriden from subclasses.

        """
        return self.f(*args, **kwargs)

    @property
    def name(self):
        """Full path to the task.
        Worker imports the task from this path.

        """
        if self._class_name:
            return "%s:%s.%s" % (
                self._module_name, self._class_name, self.f.__name__)
        else:
            return "%s:%s" % (self._module_name, self.f.__name__)

    @property
    def _module_name(self):
        """Module name of the function wrapped."""
        name = self.f.__module__
        if name == '__main__':
            name = importer.get_main_module().name
        return name

    @property
    def _class_name(self):
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
