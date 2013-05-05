import Queue
from pprint import pformat
from collections import namedtuple
from SocketServer import ThreadingTCPServer, BaseRequestHandler
from kuyruk.manager.messaging import message_loop

Master = namedtuple('Master', 'socket uptime')


class ManagerServer(ThreadingTCPServer):

    daemon_threads = True

    def __init__(self, host, port):
        self.sockets = {}
        ThreadingTCPServer.__init__(self, (host, port), RequestHandler)

    def get_request(self):
        client_sock, client_addr = ThreadingTCPServer.get_request(self)
        self.sockets[client_addr] = {
            'socket': client_sock,
            'actions': Queue.Queue(),
        }
        print 'self.sockets', pformat(self.sockets)
        return client_sock, client_addr

    def process_request_thread(self, request, client_address):
        ThreadingTCPServer.process_request_thread(self, request, client_address)
        self._remove_socket(client_address)

    def _remove_socket(self, client_address):
        del self.sockets[client_address]
        print 'self.sockets', pformat(self.sockets)


class RequestHandler(BaseRequestHandler):

    def handle(self):
        try:
            message_loop(self.request, self._generate_action, self._on_stats)
        except EOFError:
            print 'Client disconnected'

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
