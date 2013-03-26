import logging

from .queue import Queue
from . import loader

logger = logging.getLogger(__name__)


class TaskResult(object):

    def __getattribute__(self, item):
        raise Exception

    def __getitem__(self, item):
        raise Exception

    def __setattr__(self, key, value):
        raise Exception

    def __setitem__(self, key, value):
        raise Exception


class Task(object):

    def __init__(self, f, kuyruk, queue='kuyruk', local=False):
        self.f = f
        self.kuyruk = kuyruk
        self.queue = queue
        self.local = local

    def __repr__(self):
        return "<Task %s>" % self.fully_qualified_name

    def __call__(self, *args, **kwargs):
        fname = self.fully_qualified_name
        assert self.is_reachable(fname, self.f)
        logger.debug('fname: %s', fname)
        if self.kuyruk.eager:
            self.f(*args, **kwargs)
        else:
            queue = Queue(self.queue, self.kuyruk.connection, self.local)
            queue.send({'f': fname, 'args': args, 'kwargs': kwargs})
            queue.close()

        return TaskResult()

    @property
    def fully_qualified_name(self):
        return loader.get_fully_qualified_function_name(self.f)

    def is_reachable(self, fname, f):
        imported = loader.import_task(fname)
        return imported.f is f
