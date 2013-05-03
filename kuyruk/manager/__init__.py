"""
Web management feature

"""
import threading
from time import sleep
from SocketServer import TCPServer, ThreadingMixIn, BaseRequestHandler
from kuyruk.manager.messaging import receive_message

HOST, PORT = "localhost", 16500


class RequestHandler(BaseRequestHandler):

    def handle(self):
        while 1:
            try:
                print receive_message(self.request)
            except EOFError:
                break


class ThreadedTCPServer(ThreadingMixIn, TCPServer):

    def __init__(self, server_address, RequestHandlerClass,
                 bind_and_activate=True):
        self.sockets = {}
        TCPServer.__init__(self, server_address, RequestHandlerClass,
                           bind_and_activate)

    def get_request(self):
        client_sock, client_addr = TCPServer.get_request(self)
        self.sockets[client_addr] = client_sock
        print 'self.sockets', self.sockets
        return client_sock, client_addr

    def finish_request(self, request, client_address):
        retval = TCPServer.finish_request(self, request, client_address)
        print 'deleting on finish'
        del self.sockets[client_address]
        return retval

    def handle_error(self, request, client_address):
        print 'deleting on error'
        del self.sockets[client_address]
        return TCPServer.handle_error(self, request, client_address)


if __name__ == "__main__":
    server = ThreadedTCPServer((HOST, PORT), RequestHandler)
    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    # server_thread.daemon = True
    server_thread.start()
    print "Server loop running in thread:", server_thread.name

    try:
        while 1:
            sleep(1)
    except KeyboardInterrupt:
        server.shutdown()
