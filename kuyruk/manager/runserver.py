#!/putio/env/bin/python
import threading
from wsgiref.simple_server import make_server

from kuyruk.manager.app import create_app
from kuyruk.manager.server import ManagerServer

HOST, PORT = "localhost", 16500


def main():
    manager_server = ManagerServer(HOST, PORT)
    app = create_app(manager_server)

    server_thread = threading.Thread(target=manager_server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    print "Server loop running in thread:", server_thread.name

    httpd = make_server('', 5000, app)
    print "Serving on port 5000..."
    httpd.serve_forever()

if __name__ == "__main__":
    main()
