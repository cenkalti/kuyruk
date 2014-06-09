import os
import ast
import types
import logging
import multiprocessing


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
    See `subprocess.Popen
    <http://docs.python.org/2/library/subprocess.html#popen-constructor>`_
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

    # Scheduler Options
    ###################

    SCHEDULE = {}
    """Basic scheduler for kuyruk.
    see :class:`~kuyruk.scheduler.Scheduler` for more details."""

    SCHEDULER_FILE_NAME = 'kuyruk_scheduler'
    """Last sent tasks' timestamps are saved to this file."""

    def from_object(self, obj):
        """Load values from an object."""
        for key in dir(obj):
            if key.isupper():
                value = getattr(obj, key)
                setattr(self, key, value)
        logger.info("Config is loaded from object: %r", obj)

    def from_dict(self, d):
        """Load values from a dict."""
        for key, value in d.iteritems():
            if key.isupper():
                setattr(self, key, value)
        logger.info("Config is loaded from dict: %r", d)

    def from_pyfile(self, filename):
        """Load values from a Python file."""
        # Read the config file from a seperate process because it may contain
        # import statements doing import from user code. No user code should
        # be imported to master because they have to be imported in workers
        # after the master has forked. Otherwise newly created workers
        # cannot load new code after the master has started.
        def readfile(conn):
            logger.debug("Reading config file from seperate process...")
            try:
                globals_, locals_ = {}, {}
                execfile(filename, globals_, locals_)
                values = {}
                for key, value in locals_.iteritems():
                    if (key.isupper() and
                            not isinstance(value, types.ModuleType)):
                        values[key] = value
                conn.send(values)
                logger.debug("Config read successfully")
            except:
                logger.exception("Cannot read config")
                conn.send(None)
        parent_conn, child_conn = multiprocessing.Pipe()
        process = multiprocessing.Process(target=readfile,
                                          args=(child_conn, ))
        process.start()
        values = parent_conn.recv()
        process.join()
        assert values is not None, "Cannot load config file: %s" % filename
        self.from_dict(values)
        logger.info("Config is loaded from file: %s", filename)

    def from_env_vars(self):
        """Load values from environment variables."""
        for key, value in os.environ.iteritems():
            if key.startswith('KUYRUK_'):
                key = key.lstrip('KUYRUK_')
                self._eval_item(key, value)

    def from_cmd_args(self, args):
        """Load values from command line arguments."""
        def to_attr(option):
            return option.upper().replace('-', '_')

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
            setattr(self, key, value)
