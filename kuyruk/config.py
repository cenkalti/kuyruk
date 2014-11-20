import os
import ast
import types
import logging
import importer
import pkg_resources

import kuyruk  # required for references in docs

logger = logging.getLogger(__name__)


class Config(object):
    """Kuyruk configuration object. Default values are defined as
    class attributes.

    """
    # Connection Options
    ####################

    RABBIT_HOST = 'localhost'
    """RabbitMQ host."""

    RABBIT_PORT = 5672
    """RabbitMQ port."""

    RABBIT_VIRTUAL_HOST = '/'
    """RabbitMQ virtual host."""

    RABBIT_USER = 'guest'
    """RabbitMQ user."""

    RABBIT_PASSWORD = 'guest'
    """RabbitMQ password."""

    # Instance Options
    ##################

    TASK_CLASS = 'kuyruk.Task'
    """Implementation of all tasks unless overriden while defining tasks.
    It can be replaced with a subclass of :class:`kuyruk.Task` to customize
    the behavior.
    """

    EAGER = False
    """Run tasks in the process without sending to queue. Useful in tests."""

    # Worker Options
    ################

    WORKER_CLASS = 'kuyruk.worker.Worker'
    """Worker implementation class. It can be replaced with a subclass of
    :class:`kuyruk.worker.Worker` to customize the behavior."""

    MAX_LOAD = None
    """Pause consuming queue when the load goes above this level."""

    MAX_WORKER_RUN_TIME = None
    """Gracefully shutdown worker after running this seconds."""

    LOGGING_LEVEL = 'INFO'
    """Logging level of root logger."""

    LOGGING_CONFIG = None
    """INI style logging configuration file.
    This has precedence over ``LOGGING_LEVEL``."""

    def from_object(self, obj):
        """Load values from an object."""
        for key in dir(obj):
            if key.isupper():
                value = getattr(obj, key)
                self._setattr(key, value)
        logger.info("Config is loaded from object: %r", obj)

    def from_dict(self, d):
        """Load values from a dict."""
        for key, value in d.iteritems():
            if key.isupper():
                self._setattr(key, value)
        logger.info("Config is loaded from dict: %r", d)

    def from_pymodule(self, module_name):
        if not isinstance(module_name, basestring):
            raise TypeError
        module = importer.import_module(module_name)
        for key, value in module.__dict__.iteritems():
            if (key.isupper() and
                    not isinstance(value, types.ModuleType)):
                self._setattr(key, value)
        logger.info("Config is loaded from module: %s", module_name)

    def from_pyfile(self, filename):
        """Load values from a Python file."""
        globals_, locals_ = {}, {}
        execfile(filename, globals_, locals_)
        for key, value in locals_.iteritems():
            if (key.isupper() and
                    not isinstance(value, types.ModuleType)):
                self._setattr(key, value)
        logger.info("Config is loaded from file: %s", filename)

    def from_env_vars(self):
        """Load values from environment variables.
        Keys must start with `KUYRUK_`."""
        for key, value in os.environ.iteritems():
            if key.startswith('KUYRUK_'):
                key = key.lstrip('KUYRUK_')
                self._eval_item(key, value)

    def from_cmd_args(self, args):
        """Load values from command line arguments."""
        to_attr = lambda x: x.upper().replace('-', '_')
        for key, value in vars(args).iteritems():
            if value is not None:
                key = to_attr(key)
                self._eval_item(key, value)

    def _eval_item(self, key, value):
        if hasattr(Config, key):
            try:
                value = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                pass
            self._setattr(key, value)

    def _setattr(self, key, value):
        if not hasattr(self.__class__, key):
            raise ValueError("Unknown config key: %s" % key)
        setattr(self, key, value)


# Add additional config keys from extensions.
for entry_point in pkg_resources.iter_entry_points("kuyruk.config"):
    for key, value in entry_point.load().items():
        setattr(Config, key, value)
