import inspect
import logging

from kuyruk.queue import Queue

logger = logging.getLogger(__name__)


class Task(object):

    def __init__(self, f, kuyruk, queue='kuyruk'):
        self.f = f
        self.kuyruk = kuyruk
        self.queue_name = queue

    def __repr__(self):
        return "<Task %s>" % self.fully_qualified_name

    def __call__(self, *args, **kwargs):
        fname = self.fully_qualified_name
        logger.debug('fname: %s', fname)
        if self.kuyruk.eager:
            self.f(*args, **kwargs)
        else:
            queue = Queue(self.queue_name, self.kuyruk.connection)
            queue.send({'fname': fname, 'args': args, 'kwargs': kwargs})
            queue.close()

    @property
    def fully_qualified_name(self):
        f = self.f
        if inspect.ismethod(f):
            return f.__module__ + '.' + f.__self__.__name__ + '.' + f.__name__
        else:
            return f.__module__ + '.' + f.__name__
