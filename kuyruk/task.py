import os
import sys
import inspect
import logging

from kuyruk.queue import Queue
from kuyruk.loader import import_task

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
        self.check_reachable(fname, self.f)
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
        module_name = f.__module__
        if module_name == '__main__':
            main_module = sys.modules['__main__']
            filename = os.path.basename(main_module.__file__)
            module_name = os.path.splitext(filename)[0]
        if inspect.ismethod(f):
            return module_name + '.' + f.__self__.__name__ + '.' + f.__name__
        else:
            return module_name + '.' + f.__name__

    def check_reachable(self, fname, f):
        imported = import_task(fname)
        assert imported.f is f
