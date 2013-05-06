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

    def __init__(self, path=None):
        """Initialize Config from a module.
        Configuration options must be prefixed with "KUYRUK_".
        They are stripped when the Config object is initialized.

        """
        if path:
            self.path = path
            self._load_module()

    def reload(self):
        assert self.path
        logger.warning("Reloading config from %s", self.path)
        self._clear()
        self._load_module()

    def _load_module(self):
        module = imp.load_source('kuyruk_user_config', self.path)
        for k, v in module.__dict__.iteritems():
            if k.startswith('KUYRUK_'):
                setattr(self, k[7:], v)
        logger.info("Config is loaded from %s", self.path)

    def _clear(self):
        for k, v in self.__dict__.items():
            delattr(self, k)
