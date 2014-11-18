from __future__ import absolute_import
import os
import sys
import json
import signal
import socket
import logging
import traceback
from time import time
from uuid import uuid1
from functools import wraps
from datetime import datetime
from contextlib import contextmanager

import amqp

from kuyruk import signals, importer
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
        cls = self._cls or self.arg_class
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
        cls = self.arg_class or self._cls
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


class Task(object):
    """Calling a :class:`~kuyruk.Task` object serializes the task to JSON
    and sends it to the queue.

    """
    def __init__(self, f, kuyruk, queue='kuyruk', local=False,
                 retry=0, max_run_time=None, arg_class=None):
        self.f = f
        self.kuyruk = kuyruk
        self.queue = queue
        self.local = local
        self.retry = retry
        self.max_run_time = max_run_time
        self.arg_class = arg_class
        self._cls = None
        self._send_signal(signals.task_init)

    def __repr__(self):
        return "<Task of %r>" % self.name

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
        local = kwargs.pop('kuyruk_local', False)

        if self.kuyruk.config.EAGER:
            # Run the task in current process
            self._run(*args, **kwargs)
        else:
            self._send_to_queue(args, kwargs, host=host, local=local)

    def __get__(self, obj, objtype):
        """If the task is accessed from an instance via attribute syntax
        returns a bound task object that wraps the task itself, otherwise
        returns the task itself.

        This is done for allowing a method to be converted to task without
        modifying the client code. When a function decorated inside a class
        there is no way of accessing that class at that time because methods
        are bounded at run time when they are accessed. The trick here is that
        we set self._cls when the Task is accessed first time via attribute
        syntax.

        """
        self._cls = objtype
        if obj:
            # Class tasks needs to know what the object is so they can
            # inject that object in front of args.
            # We are returning a BoundTask instance here wrapping this task
            # that will do the injection.
            logger.debug("Creating bound task with obj=%r", obj)
            return BoundTask(self, obj)
        return self

    def _send_to_queue(self, args, kwargs, host=None, local=False):
        """
        Sends this task to queue.

        :param args: Arguments that will be passed to task on execution.
        :param kwargs: Keyword arguments that will be passed to task
            on execution.
        :param host: Send this task to specific host. ``host`` will be
            appended to the queue name.
        :param local: Send this task to this host. Hostname of current host
            will be appended to the queue name.
        :return: :const:`None`

        """
        logger.debug("Task.send_to_queueue args=%r, kwargs=%r", args, kwargs)
        queue = get_queue_name(self.queue, host=host, local=local or self.local)
        description = self._get_description(args, kwargs, queue)
        self._send_signal(signals.task_presend, args=args, kwargs=kwargs,
                          description=description)
        body = json.dumps(description)
        msg = amqp.Message(body=body)
        with self.kuyruk.channel() as ch:
            ch.queue_declare(queue=queue, durable=True, auto_delete=False)
            ch.basic_publish(msg, exchange="", routing_key=queue)
        self._send_signal(signals.task_postsend, args=args, kwargs=kwargs,
                          description=description)

    def _get_description(self, args, kwargs, queue):
        """Return the dictionary to be sent to the queue."""
        return {
            'id': uuid1().hex,
            'queue': queue,
            'args': args,
            'kwargs': kwargs,
            'module': self._module_name,
            'function': self.f.__name__,
            'class': self._class_name,
            'sender_hostname': socket.gethostname(),
            'sender_pid': os.getpid(),
            'sender_cmd': ' '.join(sys.argv),
            'sender_timestamp': datetime.utcnow().isoformat()[:19],
        }

    def _send_signal(self, sig, **data):
        """
        Sends a signal for each sender.
        This allows the user to register for a specific sender.

        """
        for sender in (self, self.kuyruk):
            sig.send(sender, task=self, **data)

    @object_to_id
    def apply(self, *args, **kwargs):
        """Runs the wrapped function and signal handlers as if it is run by
        a worker.
        If task has a `retry` property it will be retried on failure.
        If task has a `max_run_time` property the task will not be allowed to
        run more than that.
        Calls :func:`~kuyruk.Task.run` to run the wrapped function.
        """
        logger.debug("Task.apply args=%r, kwargs=%r", args, kwargs)
        return self._run(*args, **kwargs)

    # This function is called by worker to run the task.
    @profile
    @id_to_object
    def _run(self, *args, **kwargs):
        def send_signal(sig, **extra):
            self._send_signal(sig, args=args, kwargs=kwargs, **extra)

        logger.debug("Applying %r, args=%r, kwargs=%r", self, args, kwargs)

        send_signal(signals.task_prerun)
        try:
            tries = 1 + self.retry
            while 1:
                tries -= 1
                try:
                    with time_limit(self.max_run_time or 0):
                        self.run(*args, **kwargs)
                except Exception:
                    traceback.print_exc()
                    send_signal(signals.task_error, exc_info=sys.exc_info())
                    if tries <= 0:
                        raise
                else:
                    break
        except Exception:
            send_signal(signals.task_failure, exc_info=sys.exc_info())
            raise
        else:
            send_signal(signals.task_success)
        finally:
            send_signal(signals.task_postrun)

    def run(self, *args, **kwargs):
        """
        Calls the wrapped function.
        :func:`~kuyruk.Task.apply` calls this method internally so
        you may override this method from a subclass to change the behavior.

        """
        self.f(*args, **kwargs)

    @property
    def name(self):
        """Full path to the task. Consists of module, class and function name.
        Workers find and import tasks by this path.

        """
        if self._class_name:
            return "%s:%s.%s" % \
                   (self._module_name, self._class_name, self.f.__name__)
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
        if self._cls:
            return self._cls.__name__


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


def get_queue_name(name, host=None, local=False):
    if local:
        host = socket.gethostname()
    if host:
        name = "%s.%s" % (name, host)
    return name
