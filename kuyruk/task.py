import logging

from kuyruk.queue import Queue
from kuyruk import loader

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
        assert self.is_reachable(fname, self.f)
        logger.debug('fname: %s', fname)
        if self.kuyruk.eager:
            self.f(*args, **kwargs)
        else:
            queue = Queue(self.queue_name, self.kuyruk.connection)
            queue.send({'f': fname, 'args': args, 'kwargs': kwargs})
            queue.close()

    @property
    def fully_qualified_name(self):
        return loader.get_fully_qualified_function_name(self.f)

    def is_reachable(self, fname, f):
        imported = loader.import_task(fname)
        return imported.f is f
