import multiprocessing


class Process(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._queue = multiprocessing.Queue(maxsize=1)
        self._result = None

    def run(self):
        try:
            result = self._target(*self._args, **self._kwargs)
            self._queue.put((result, None))
        except Exception as exc:
            self._queue.put((None, exc))

    @property
    def result(self):
        self._result = self._queue.get()
        return self._result
