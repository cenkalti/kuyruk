"""
This module is called "events" instead of "signals" not to confuse with OS
level signals.

"""
from blinker import Namespace

_signals = Namespace()

task_prerun = _signals.signal('task-prerun')
task_postrun = _signals.signal('task-postrun')
task_success = _signals.signal('task-success')
task_failure = _signals.signal('task-failure')
task_presend = _signals.signal('task-presend')
task_postsend = _signals.signal('task-postsend')


class EventMixin(object):
    """
    This mixin class contains some decorator methods for wrapping handler
    functions to run on certain signals.

    """
    def on_prerun(self, f):
        """
        Registers a function to run before processing the task.
        If the registered function raises an exception the task will be
        considered as failed.

        """
        self.connect_signal(task_prerun, f)
        return f

    def on_postrun(self, f):
        """
        Registers a function to run after processing the task.
        The registered function will run regardless of condition that
        the task is successful or failed. You can attach some cleanup code
        here i.e. closing sockets or deleting files.

        """
        self.connect_signal(task_postrun, f)
        return f

    def on_success(self, f):
        """
        Registers a function to run after the task has returned.
        The registered function will only be run if the task is successful.

        """
        self.connect_signal(task_success, f)
        return f

    def on_failure(self, f):
        """
        Registers a function to run if task raises an exception.

        """
        self.connect_signal(task_failure, f)
        return f

    def on_presend(self, f):
        """
        Registers a function to run before the task is sent to the queue.
        Note that this is executed in the client process,
        the one sending the task, not in the worker.

        """
        self.connect_signal(task_presend, f)
        return f

    def on_postsend(self, f):
        """
        Registers a function to run after the task is sent to the queue.
        Note that this is executed in the client process,
        the one sending the task, not in the worker.

        """
        self.connect_signal(task_postsend, f)
        return f

    def connect_signal(self, signal, handler):
        signal.connect(handler, sender=self, weak=False)
