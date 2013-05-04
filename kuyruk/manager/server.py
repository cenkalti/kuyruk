import Queue
from pprint import pformat
from collections import namedtuple
from SocketServer import BaseRequestHandler, ThreadingMixIn, TCPServer
from kuyruk.manager.messaging import message_loop

Master = namedtuple('Master', 'socket uptime')


class RequestHandler(BaseRequestHandler):

    def handle(self):
        message_loop(self.request, self._generate_action, self._on_stats)

    def _generate_action(self):
        try:
            return self.struct['actions'].get_nowait()
        except Queue.Empty:
            pass

    def _on_stats(self, sock, stats):
        print self.client_address, pformat(stats)
        self.struct['stats'] = stats

    @property
    def struct(self):
        return self.server.sockets[self.client_address]


class ThreadedTCPServer(ThreadingMixIn, TCPServer):

    daemon_threads = True

    def __init__(self, server_address, RequestHandlerClass,
                 bind_and_activate=True):
        self.sockets = {}
        TCPServer.__init__(self, server_address, RequestHandlerClass,
                           bind_and_activate)

    def get_request(self):
        client_sock, client_addr = TCPServer.get_request(self)
        self.sockets[client_addr] = {
            'socket': client_sock,
            'actions': Queue.Queue(),
        }
        print 'self.sockets', pformat(self.sockets)
        return client_sock, client_addr

    def finish_request(self, request, client_address):
        TCPServer.finish_request(self, request, client_address)
        print 'deleting on finish'
        self._remove_socket(client_address)

    def handle_error(self, request, client_address):
        print 'deleting on error'
        self._remove_socket(client_address)
        TCPServer.handle_error(self, request, client_address)

    def _remove_socket(self, client_address):
        del self.sockets[client_address]
        print 'self.sockets', pformat(self.sockets)
