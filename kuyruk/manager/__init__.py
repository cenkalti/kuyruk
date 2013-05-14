import logging
from werkzeug.serving import run_simple
from kuyruk.helpers import start_daemon_thread
from kuyruk.manager.app import app
from kuyruk.manager.server import ManagerServer

logger = logging.getLogger(__name__)


def run(config, args):
    if config.MANAGER_HOST is None:
        config.MANAGER_HOST = '127.0.0.1'

    manager = ManagerServer(config.MANAGER_HOST, config.MANAGER_PORT)
    manager_thread = start_daemon_thread(manager.serve_forever)
    logger.info("Manager running in thread: %s", manager_thread.name)

    app.manager = manager
    app.debug = True
    run_simple(config.MANAGER_HOST, config.MANAGER_HTTP_PORT,
               app, threaded=True, use_debugger=True)
