class Task(object):

    def __init__(self, f, kuyruk):
        self.f = f
        self.kuyruk = kuyruk

    def __call__(self, *args, **kwargs):
        # TODO send to queue instead of calling
        print 'calling'
        return self.f(*args, **kwargs)
