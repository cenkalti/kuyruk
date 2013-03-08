import logging

from kuyruk.queue import Queue

logger = logging.getLogger(__name__)


class Task(object):

    def __init__(self, f, kuyruk):
        self.f = f
        self.kuyruk = kuyruk

    def __call__(self, *args, **kwargs):
        fname = self.f.__module__ + "." + self.f.__name__
        logger.debug('fname: %s', fname)
        if self.kuyruk.eager:
            self.f(*args, **kwargs)
        else:
            queue = Queue('kuyruk', self.kuyruk.connection)
            queue.send({'fname': fname, 'args': args, 'kwargs': kwargs})
            queue.close()
