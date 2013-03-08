from kuyruk.queue import Queue


class Task(object):

    def __init__(self, f, kuyruk):
        self.f = f
        self.kuyruk = kuyruk

    def __call__(self, *args, **kwargs):
        queue = Queue('kuyruk', self.kuyruk.connection)
        queue.send({'args': args, 'kwargs': kwargs})
        queue.close()
        # return self.f(*args, **kwargs)
