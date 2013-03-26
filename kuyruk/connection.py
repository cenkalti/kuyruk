import pika
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def require_open(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        if not self.is_open:
            self.open()
        return f(self, *args, **kwargs)
    return inner


class LazyConnection(object):

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self._connection = None

    @property
    def is_open(self):
        return self._connection is not None and self._connection.is_open

    def open(self):
        assert not self.is_open
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host, port=self.port, credentials=credentials)
        self._connection = pika.BlockingConnection(parameters)
        logger.info('Connected to RabbitMQ')

    @require_open
    def channel(self):
        return self._connection.channel()

    def close(self):
        if self.is_open:
            self._connection.close()
            logger.info('Connection closed')
        else:
            logger.debug('Not connected')
