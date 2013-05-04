import socket

from kuyruk.manager.messaging import message_loop
from kuyruk.helpers import start_daemon_thread


class ManagerClientMixin(object):

    def start_manager_client(self, host, port, stop_event):
        start_daemon_thread(self.run_manager_client, (host, port, stop_event))

    def run_manager_client(self, host, port, stop_event):
        """Connect to manager and read/write messages from/to socket."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
            message_loop(
                sock,
                self.generate_message,
                self.on_message,
                stop_event=stop_event)
        finally:
            sock.close()

    def generate_message(self):
        """Callback function for message to be sent to manager."""
        pass

    def on_message(self, sock, action):
        """Run the function sent by the manager."""
        f, args, kwargs = action
        print f, args, kwargs
        f = getattr(self, f)
        f(*args, **kwargs)
