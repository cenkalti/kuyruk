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

    def __init__(self, obj=None):
        """Populate from obj. obj may be a path to a module, a dict or
        a dict. If the key is not found in obj, default is used. Config
        keys must be prefixed with "KUYRUK_". They are stripped when
        the Config object is initialized.

        """
        if obj:
            if isinstance(obj, dict):
                self._load_dict(obj)
            elif isinstance(obj, basestring):
                self._load_module(obj)
            else:
                self._load_object(obj)

    def reload(self):
        logger.warning("Reloading config from %s", self.path)
        self._load_module(self.path)

    def _load_dict(self, new_dict):
        self.clear()
        for k, v in new_dict.iteritems():
            if k.startswith('KUYRUK_'):
                setattr(self, k[7:], v)

    def _load_module(self, path):
        module = imp.load_source('kuyruk_user_config', path)
        self._load_object(module)
        logger.info("Config is loaded from %s", path)
        self.path = path  # Save for reloading later

    def _load_object(self, obj):
        self._load_dict(obj.__dict__)

    def clear(self):
        for k, v in self.__dict__.items():
            delattr(self, k)
