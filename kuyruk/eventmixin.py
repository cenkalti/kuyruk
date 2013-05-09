from __future__ import absolute_import
import logging
from kuyruk import signals

logger = logging.getLogger(__name__)


class EventMixin(object):

    def before_task(self, f):
        signals.before_task.connect(f, sender=self)
        return f

    def after_task(self, f):
        signals.after_task.connect(f, sender=self)
        return f

    def on_return(self, f):
        signals.on_return.connect(f, sender=self)
        return f

    def on_exception(self, f):
        signals.on_exception.connect(f, sender=self)
        return f
