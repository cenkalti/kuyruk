import json
import struct
from time import sleep


def receive_message(sock):
    size = sock.recv(4)
    if not size:
        raise EOFError
    size = struct.unpack('I', size)[0]
    message = sock.recv(size)
    return json.loads(message)


def send_message(sock, message):
    message = json.dumps(message)
    size = len(message)
    size = struct.pack('I', size)
    sock.sendall(size)
    sock.sendall(message)
    sleep(60)
