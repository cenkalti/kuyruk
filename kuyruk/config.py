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
    IMPORTS = []
    EAGER = False
    MAX_LOAD = None
    MAX_RUN_TIME = None
    SAVE_FAILED_TASKS = False
    WORKERS = {}
    MANAGER_HOST = None
    MANAGER_PORT = 16501
    MANAGER_HTTP_PORT = 16500

    def from_object(self, obj):
        """Populate Config from an object.
        Configuration options must be prefixed with "KUYRUK_".
        They are stripped when the Config object is populated.

        """
        for key in dir(obj):
            if key.startswith('KUYRUK_'):
                value = getattr(obj, key)
                setattr(self, key[7:], value)
        logger.info("Config is loaded from %r", obj)

    def from_pyfile(self, filename):
        d = imp.new_module('kuyruk_config')
        d.__file__ = filename
        try:
            execfile(filename, d.__dict__)
        except IOError as e:
            e.strerror = 'Unable to load configuration file (%s)' % e.strerror
            raise
        self.from_object(d)
