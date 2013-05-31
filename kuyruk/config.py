import os
import imp
import logging

logger = logging.getLogger(__name__)


class Config(object):
    """Kuyruk configuration object. Default values are defined as
    class attributes.

    """
    # Worker Options
    ################

    WORKER_CLASS = 'kuyruk.worker.Worker'
    """Worker implementation class. It can be replaced with a subclass of
    :class:`~kuyruk.worker.Worker` to change specific behavior."""

    IMPORT_PATH = None
    """Worker imports tasks from this directory."""

    IMPORTS = []
    """By default worker imports the task modules lazily when it receive a
    task from the queue. If you specify the modules here they will be
    imported when the worker is started."""

    EAGER = False
    """Run tasks in the process without sending to queue. Useful in tests."""

    MAX_LOAD = None
    """Stop consuming queue when the load goes above this level."""

    MAX_WORKER_RUN_TIME = None
    """Gracefully shutdown worker after running this seconds.
    Master will detect that the worker is exited and will spawn a new
    worker with identical config.
    Can be used to force loading of new application code."""

    MAX_TASK_RUN_TIME = None
    """Fail the task if it takes more than this seconds."""

    SAVE_FAILED_TASKS = False
    """Save failed tasks to a queue (named kuyruk_failed) for inspecting and
    requeueing later."""

    QUEUES = {}
    """You can specify the hostnames and correspondant queues here.
    Master starts workers by getting the list of queues from this dictionary
    by hostname. If the hostname is not found in the keys then the master
    start a single worker to run on default queue (``kuyruk``).

    Keys are hostnames (socket.gethostname()), values are comma seperated
    list of queue names.

    Example::

        {'host1.example.com': 'a, 2*b'}

    host1 will run 3 worker processes; 1 for "a" and 2 for "b" queue."""

    DEFAULT_QUEUES = 'kuyruk, @kuyruk'
    """If the hostname is not found in :attr:`~kuyruk.Config.QUEUES`
    this value will be used as default."""

    LOGGING_LEVEL = 'INFO'
    """Logging level of root logger."""

    LOGGING_CONFIG = None
    """INI style logging configuration file.
    This has pecedence over ``LOGGING_LEVEL``."""

    SENTRY_DSN = None
    """Send exceptions to Sentry. Raven must be installed in order that
    this feature to work."""

    SENTRY_PROJECT_URL = None
    """Sentry project URL. Required to generate links to Sentry in Manager."""

    CLOSE_FDS = True
    """Patch subprocess.Popen constructor to always set close_fds=True.
    See `subprocess.Popen <http://docs.python.org/2/library/subprocess.html#popen-constructor>`_
    for additional information."""

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

    REDIS_HOST = 'localhost'
    """Redis host."""

    REDIS_PORT = 6379
    """Redis port."""

    REDIS_DB = 0
    """Redis database."""

    REDIS_PASSWORD = None
    """Redis password."""

    # Manager Options
    #################

    MANAGER_HOST = None
    """Manager host that the workers will connect and send stats."""

    MANAGER_PORT = 16501
    """Manager port that the workers will connect and send stats."""

    MANAGER_HTTP_PORT = 16500
    """Manager HTTP port that the Flask application will run on."""

    def from_object(self, obj):
        """Loads values from an object."""
        for key in dir(obj):
            if key.isupper():
                value = getattr(obj, key)
                setattr(self, key, value)
        logger.info("Config is loaded from %r", obj)

    def from_pyfile(self, filename):
        """Loads values from a Python file."""
        d = imp.new_module('kuyruk_config')
        d.__file__ = filename
        try:
            execfile(filename, d.__dict__)
        except IOError as e:
            e.strerror = 'Unable to load configuration file (%s)' % e.strerror
            raise
        self.from_object(d)

    def export(self, file_or_path):
        """Exports values to a file."""
        if isinstance(file_or_path, file):
            f = file_or_path
        elif isinstance(file_or_path, int):
            f = os.fdopen(file_or_path, 'w')
        elif isinstance(file_or_path, basestring):
            f = open(file_or_path, 'w')
        else:
            raise TypeError("Argument must be a fd, file object or path: %r" %
                            file_or_path)

        try:
            for attr in dir(self):
                if attr.isupper() and not attr.startswith('_'):
                    value = getattr(self, attr)
                    f.write("%s = %r\n" % (attr, value))
        finally:
            f.close()
