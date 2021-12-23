import multiprocessing as mp


class Process(mp.Process):
    def __init__(self, *args, **kwargs):
        mp.Process.__init__(self, *args, **kwargs)

        # this pipe has a limited size buffer
        # if there is no consumers, it will hang after buffer is full
        self._pconn, self._cconn = mp.Pipe()
        self._result = None

    def run(self):
        try:
            result = self._target(*self._args, **self._kwargs)
            self._cconn.send((result, None))
        except Exception as exc:
            self._cconn.send((None, exc))

    @property
    def result(self):
        if self._pconn.poll():
            self._result = self._pconn.recv()
        return self._result
