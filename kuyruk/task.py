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
from kuyruk.exceptions import Timeout

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


class Task(object):
    """Calling a :class:`~kuyruk.Task` object serializes the task to JSON
    and sends it to the queue.

    """
    def __init__(self, f, kuyruk, queue='kuyruk', local=False,
                 retry=0, max_run_time=None):
        self.f = f
        self.kuyruk = kuyruk
        self.queue = queue
        self.local = local
        self.retry = retry
        self.max_run_time = max_run_time
        self._send_signal(signals.task_init)

    def __repr__(self):
        return "<Task of %r>" % self.name

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
            self.apply(*args, **kwargs)
        else:
            self._send_to_queue(args, kwargs, host=host, local=local)

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
        logger.debug("Task.send_to_queue args=%r, kwargs=%r", args, kwargs)
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
            'sender_hostname': socket.gethostname(),
            'sender_pid': os.getpid(),
            'sender_cmd': ' '.join(sys.argv),
            'sender_timestamp': datetime.utcnow().isoformat()[:19],
        }

    def _send_signal(self, sig, **data):
        sig.send(self.kuyruk, task=self, **data)

    @profile
    def apply(self, *args, **kwargs):
        """Called by workers to run the wrapped function.
        You may call it yourself if you want to run the task in current process
        without sending to the queue.

        If task has a `retry` property it will be retried on failure.

        If task has a `max_run_time` property the task will not be allowed to
        run more than that.
        """
        def send_signal(sig, **extra):
            self._send_signal(sig, args=args, kwargs=kwargs, **extra)

        logger.debug("Applying %r, args=%r, kwargs=%r", self, args, kwargs)

        send_signal(signals.task_preapply)
        try:
            tries = 1 + self.retry
            while 1:
                tries -= 1
                send_signal(signals.task_prerun)
                try:
                    with time_limit(self.max_run_time or 0):
                        self.f(*args, **kwargs)
                except Exception:
                    traceback.print_exc()
                    send_signal(signals.task_error, exc_info=sys.exc_info())
                    if tries <= 0:
                        raise
                else:
                    break
                finally:
                    send_signal(signals.task_postrun)
        except Exception:
            send_signal(signals.task_failure, exc_info=sys.exc_info())
            raise
        else:
            send_signal(signals.task_success)
        finally:
            send_signal(signals.task_postapply)

    @property
    def name(self):
        """Full path to the task in the form of `<module>.<function>`.
        Workers find and import tasks by this path.

        """
        return "%s:%s" % (self._module_name, self.f.__name__)

    @property
    def _module_name(self):
        """Module name of the wrapped function."""
        name = self.f.__module__
        if name == '__main__':
            name = importer.get_main_module().name
        return name


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
    if host:
        return "%s.%s" % (name, host)
    if local:
        return "%s.%s" % (name, socket.gethostname())
    return name
