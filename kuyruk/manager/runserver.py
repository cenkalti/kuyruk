#!/putio/env/bin/python
import eventlet
eventlet.monkey_patch()

import threading
from eventlet import wsgi
from kuyruk.manager.app import create_app
from kuyruk.manager.server import RequestHandler, ThreadedTCPServer

HOST, PORT = "localhost", 16500


def main():
    server = ThreadedTCPServer((HOST, PORT), RequestHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    print "Server loop running in thread:", server_thread.name
    app = create_app(server)
    wsgi.server(eventlet.listen(('', 5000)), app, debug=True)


if __name__ == "__main__":
    main()
