import imp
import logging

logger = logging.getLogger(__name__)


class Config(object):
    """Kuyruk configuration object"""

    # Defaults
    RABBIT_HOST = 'localhost'
    RABBIT_PORT = 5672
    RABBIT_USER = 'guest'
    RABBIT_PASSWORD = 'guest'
    IMPORT_PATH = None
    EAGER = False
    MAX_LOAD = None
    MAX_RUN_TIME = None
    SAVE_FAILED_TASKS = False
    WORKERS = {}
    MANAGER_HOST = None
    MANAGER_PORT = 16501
    MANAGER_HTTP_PORT = 16500

    def __init__(self, module=None):
        """Initialize Config from a module.
        Configuration options must be prefixed with "KUYRUK_".
        They are stripped when the Config object is initialized.

        """
        if module is None:
            module = imp.new_module('kuyruk_config')

        self.module = module
        self._load_module()

    @classmethod
    def from_path(cls, path=None):
        if path:
            module = imp.load_source('kuyruk_config', path)
            return cls(module)
        else:
            return cls()

    def _load_module(self):
        for k, v in self.module.__dict__.iteritems():
            if k.startswith('KUYRUK_'):
                setattr(self, k[7:], v)
        logger.info("Config is loaded from %r", self.module)
