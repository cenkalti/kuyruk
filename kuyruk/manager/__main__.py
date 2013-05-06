#!/putio/env/bin/python
import logging
from werkzeug.serving import run_simple
from kuyruk.helpers import start_daemon_thread
from kuyruk.manager.app import app
from kuyruk.optionparser import OptionParser
from kuyruk.manager.server import ManagerServer

logger = logging.getLogger(__name__)


def run(host, manager_port, http_port):
    manager = ManagerServer(host, manager_port)
    manager_thread = start_daemon_thread(manager.serve_forever)
    logger.info("Manager running in thread: %s", manager_thread.name)

    app.manager = manager
    app.debug = True
    run_simple(host, http_port, app, threaded=True, use_debugger=True)


def main():
    parser = OptionParser()
    # Options below overrides config
    parser.add_option('--host')
    parser.add_option('--port', type='int')
    parser.add_option('--http-port', type='int')
    options, args = parser.parse_args()
    config = parser.config
    if options.host:
        config.MANAGER_HOST = options.host
    if options.port:
        config.MANAGER_PORT = options.port
    if options.http_port:
        config.MANAGER_HTTP_PORT = options.http_port

    if config.MANAGER_HOST is None:
        config.MANAGER_HOST = '127.0.0.1'

    run(config.MANAGER_HOST, config.MANAGER_PORT, config.MANAGER_HTTP_PORT)


if __name__ == "__main__":
    main()
