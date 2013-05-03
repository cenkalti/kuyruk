import socket
from kuyruk.manager.messaging import send_message

HOST, PORT = "localhost", 16500

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    sock.connect((HOST, PORT))
    send_message(sock, {'a': 101})
finally:
    sock.close()
