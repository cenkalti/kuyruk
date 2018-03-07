import os
import ast
import types
import logging
import pkg_resources
from typing import Dict, Any, Union  # noqa

from kuyruk import importer

import kuyruk  # noqa; required for references in docs

logger = logging.getLogger(__name__)


class Config:
    """Kuyruk configuration object. Default values are defined as
    class attributes. Additional attributes may be added by extensions.

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

    RABBIT_CONNECT_TIMEOUT = 5
    RABBIT_READ_TIMEOUT = 5
    RABBIT_WRITE_TIMEOUT = 5

    # Instance Options
    ##################

    EAGER = False
    """Run tasks in the process without sending to queue. Useful in tests."""

    # Worker Options
    ################

    WORKER_MAX_LOAD = None
    """Pause consuming queue when the load goes above this level."""

    WORKER_MAX_RUN_TIME = None
    """Gracefully shutdown worker after running this seconds."""

    WORKER_LOGGING_LEVEL = 'INFO'
    """Logging level of root logger."""

    def from_object(self, obj: Union[str, Any]) -> None:
        """Load values from an object."""
        if isinstance(obj, str):
            obj = importer.import_object_str(obj)

        for key in dir(obj):
            if key.isupper():
                value = getattr(obj, key)
                self._setattr(key, value)

        logger.info("Config is loaded from object: %r", obj)

    def from_dict(self, d: Dict[str, Any]) -> None:
        """Load values from a dict."""
        for key, value in d.items():
            if key.isupper():
                self._setattr(key, value)

        logger.info("Config is loaded from dict: %r", d)

    def from_pymodule(self, name: str) -> None:
        module = importer.import_module(name)
        for key, value in module.__dict__.items():
            if (key.isupper() and not isinstance(value, types.ModuleType)):
                self._setattr(key, value)

        logger.info("Config is loaded from module: %s", name)

    def from_pyfile(self, filename: str) -> None:
        """Load values from a Python file."""
        globals_ = {}  # type: Dict[str, Any]
        locals_ = {}  # type: Dict[str, Any]
        with open(filename, "rb") as f:
            exec(compile(f.read(), filename, 'exec'), globals_, locals_)

        for key, value in locals_.items():
            if (key.isupper() and not isinstance(value, types.ModuleType)):
                self._setattr(key, value)

        logger.info("Config is loaded from file: %s", filename)

    def from_env_vars(self) -> None:
        """Load values from environment variables.
        Keys must start with `KUYRUK_`."""
        for key, value in os.environ.items():
            if key.startswith('KUYRUK_'):
                key = key[7:]
                if hasattr(Config, key):
                    try:
                        value = ast.literal_eval(value)
                    except (ValueError, SyntaxError):
                        pass

                    self._setattr(key, value)

    def _setattr(self, key: str, value: Any) -> None:
        if not hasattr(self.__class__, key):
            raise ValueError("Unknown config key: %s" % key)

        setattr(self, key, value)


# Add additional config keys from extensions.
for entry_point in pkg_resources.iter_entry_points("kuyruk.config"):
    for k, v in entry_point.load().items():
        setattr(Config, k, v)
