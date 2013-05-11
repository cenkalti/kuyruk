from __future__ import absolute_import
import logging
from kuyruk import signals

logger = logging.getLogger(__name__)


class EventMixin(object):
    """This mixin class contains some decorator methods for wrapping handler
    functions to run on certain signals.

    """
    def before_task(self, f):
        """Registers a function to run before processing the task.
        If the registered function raises an exception the task will be
        considered as failed.

        """
        self.connect_signal(signals.before_task, f)
        return f

    def after_task(self, f):
        """Registers a function to run after processing the task.
        The registered function will run regardless of condition that
        the task is successful or failed. You can attach some cleanup code
        here i.e. closing sockets or deleting files.

        """
        self.connect_signal(signals.after_task, f)
        return f

    def on_return(self, f):
        """Registers a function to run after the task has returned.
        The registered function will only be run if the task is successful."""
        self.connect_signal(signals.on_return, f)
        return f

    def on_exception(self, f):
        """Registers a function to run if task raises an exception."""
        self.connect_signal(signals.on_exception, f)
        return f

    def connect_signal(self, signal, handler):
        signal.connect(handler, sender=self, weak=False)
