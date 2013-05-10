from __future__ import absolute_import
import logging
from kuyruk import signals
from functools import wraps

logger = logging.getLogger(__name__)


class EventMixin(object):
    """This mixin class contains some decorator methods for wrapping handler
    functions to run on certain signals.

    """
    def before_task(self, f):
        f = hide_sender(f)
        signals.before_task.connect(f, sender=self)
        return f

    def after_task(self, f):
        f = hide_sender(f)
        signals.after_task.connect(f, sender=self)
        return f

    def on_return(self, f):
        f = hide_sender(f)
        signals.on_return.connect(f, sender=self)
        return f

    def on_exception(self, f):
        f = hide_sender(f)
        signals.on_exception.connect(f, sender=self)
        return f


def hide_sender(f):
    @wraps(f)
    def inner(sender, *args, **kwargs):
        return f(*args, **kwargs)
    return inner
