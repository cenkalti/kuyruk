import logging
from werkzeug.serving import run_simple
from kuyruk.helpers import start_daemon_thread
from kuyruk.manager.app import app
from kuyruk.manager.server import ManagerServer

logger = logging.getLogger(__name__)


class Manager(object):

    def __init__(self, config):
        self.config = config
        if self.config.MANAGER_HOST is None:
            self.config.MANAGER_HOST = '127.0.0.1'

        self.server = ManagerServer(
            self.config.MANAGER_HOST,
            self.config.MANAGER_PORT)

        app.config.from_object(self.config)
        app.manager = self.server
        app.debug = True

    def run(self):
        manager_thread = start_daemon_thread(self.server.serve_forever)
        logger.info("Manager running in thread: %s", manager_thread.name)

        run_simple(self.config.MANAGER_HOST, self.config.MANAGER_HTTP_PORT,
                   app, threaded=True, use_debugger=True)
