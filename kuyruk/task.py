import logging

from .queue import Queue
from . import loader
from .connection import LazyConnection

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
        if self.kuyruk.config.EAGER:
            self.f(*args, **kwargs)
        else:
            connection = LazyConnection(
                self.kuyruk.config.RABBIT_HOST, self.kuyruk.config.RABBIT_PORT,
                self.kuyruk.config.RABBIT_USER, self.kuyruk.config.RABBIT_PASSWORD)
            channel = connection.channel()
            with connection:
                with channel:
                    queue = Queue(self.queue, channel, self.local)
                    queue.send({'f': fname, 'args': args, 'kwargs': kwargs})

        return TaskResult()

    @property
    def fully_qualified_name(self):
        return loader.get_fully_qualified_function_name(self.f)

    def is_reachable(self, fname, f):
        imported = loader.import_task(fname)
        return imported.f is f
