from pprint import pprint, pformat
from collections import namedtuple
from SocketServer import BaseRequestHandler, ThreadingMixIn, TCPServer
from kuyruk.manager.messaging import receive_message

Master = namedtuple('Master', 'socket uptime')


class RequestHandler(BaseRequestHandler):

    def handle(self):
        while 1:
            try:
                stats = receive_message(self.request)
                print self.client_address, pformat(stats)
                self.server.sockets[self.client_address]['stats'] = stats
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
        self.sockets[client_addr] = {'socket': client_sock}
        print 'self.sockets', pformat(self.sockets)
        return client_sock, client_addr

    def finish_request(self, request, client_address):
        retval = TCPServer.finish_request(self, request, client_address)
        print 'deleting on finish'
        self._remove_socket(client_address)
        return retval

    def handle_error(self, request, client_address):
        print 'deleting on error'
        self._remove_socket(client_address)
        return TCPServer.handle_error(self, request, client_address)

    def _remove_socket(self, client_address):
        del self.sockets[client_address]
        print 'self.sockets', pformat(self.sockets)
