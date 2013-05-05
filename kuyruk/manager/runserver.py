#!/putio/env/bin/python
import logging
import threading
from werkzeug.serving import run_simple
from kuyruk.manager.app import create_app
from kuyruk.manager.server import ManagerServer

HOST, PORT = "localhost", 16500

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.DEBUG)
    manager = ManagerServer(HOST, PORT)
    manager_thread = threading.Thread(target=manager.serve_forever)
    manager_thread.daemon = True
    manager_thread.start()
    print "Manager running in thread:", manager_thread.name

    app = create_app(manager)
    app.debug = True
    run_simple('0.0.0.0', 5000, app, threaded=True, use_debugger=True)

if __name__ == "__main__":
    main()
