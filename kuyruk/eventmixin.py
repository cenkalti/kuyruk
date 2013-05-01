from __future__ import absolute_import
import logging

logger = logging.getLogger(__name__)


class EventMixin(object):

    def __init__(self):
        self.before_task_functions = []
        self.after_task_functions = []
        self.after_return_functions = []
        self.on_exception_functions = []

    def before_task(self, f):
        self.before_task_functions.append(f)
        return f

    def after_task(self, f):
        self.after_task_functions.append(f)
        return f

    def after_return(self, f):
        self.after_return_functions.append(f)
        return f

    def on_exception(self, f):
        self.on_exception_functions.append(f)
        return f
