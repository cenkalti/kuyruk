import Queue
from pprint import pformat
from functools import total_ordering
from SocketServer import ThreadingTCPServer, BaseRequestHandler

from kuyruk.manager.messaging import message_loop


class ManagerServer(ThreadingTCPServer):

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, host, port):
        self.clients = {}
        ThreadingTCPServer.__init__(self, (host, port), RequestHandler)

    def get_request(self):
        # Calls accept inside
        client_sock, client_addr = ThreadingTCPServer.get_request(self)

        # Hold a reference to our new client
        self.clients[client_addr] = Client(client_sock)
        print 'self.clients', pformat(self.clients)

        return client_sock, client_addr

    def process_request_thread(self, request, client_address):
        try:
            ThreadingTCPServer.process_request_thread(
                self, request, client_address)
        finally:
            # Client is no longer connected, remove the reference
            del self.clients[client_address]
            print 'self.clients', pformat(self.clients)


class RequestHandler(BaseRequestHandler):

    def handle(self):
        try:
            message_loop(self.request, self._generate_action, self._on_stats)
        except EOFError:
            print 'Client disconnected'

    def _generate_action(self):
        try:
            return self.struct.actions.get_nowait()
        except Queue.Empty:
            pass

    def _on_stats(self, sock, stats):
        print 'Received stats from', self.client_address
        self.struct.stats = stats

    @property
    def struct(self):
        return self.server.clients[self.client_address]


@total_ordering
class Client(object):

    def __init__(self, socket):
        self.socket = socket
        self.stats = {}
        self.actions = Queue.Queue()

    def __lt__(self, other):
        return self.sort_key < other.sort_key

    @property
    def sort_key(self):
        order = ('hostname', 'queue', 'uptime', 'pid')
        return tuple(self.get_stat(attr) for attr in order)

    def get_stat(self, name):
        return self.stats.get(name, None)
