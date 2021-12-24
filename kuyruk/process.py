import multiprocessing
import signal
import sys


class Process(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._queue = multiprocessing.Queue(maxsize=1)
        self._result = None

    def run(self):
        try:
            self._clear_signal_handlers()
            result = self._target(*self._args, **self._kwargs)
            self._queue.put((result, None))
        except Exception as exc:
            self._queue.put((None, exc))
        finally:
            self._queue.close()

    def _clear_signal_handlers(self):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGHUP, signal.SIG_DFL)
        signal.signal(signal.SIGUSR2, signal.SIG_DFL)

    @property
    def result(self):
        if not self._result:
            self._result = self._queue.get()
        return self._result
