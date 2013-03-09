import logging

from kuyruk.queue import Queue

logger = logging.getLogger(__name__)


class Task(object):

    def __init__(self, f, kuyruk):
        self.f = f
        self.kuyruk = kuyruk

    def __repr__(self):
        return "<Task %s>" % self.fully_qualified_name

    def __call__(self, *args, **kwargs):
        fname = self.fully_qualified_name
        logger.debug('fname: %s', fname)
        if self.kuyruk.eager:
            self.f(*args, **kwargs)
        else:
            queue = Queue('kuyruk', self.kuyruk.connection)
            queue.send({'fname': fname, 'args': args, 'kwargs': kwargs})
            queue.close()

    @property
    def fully_qualified_name(self):
        return "%s.%s" % (self.f.__module__, self.f.__name__)
