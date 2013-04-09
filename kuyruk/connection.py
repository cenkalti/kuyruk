import logging
from functools import wraps

import pika

logger = logging.getLogger(__name__)


def require_open(f):
    """Decorator for calling open() before invoking the wrapped function"""
    @wraps(f)
    def inner(self, *args, **kwargs):
        if not self.is_open:
            self.open()
        return f(self, *args, **kwargs)
    return inner


class LazyBase(object):

    def __init__(self):
        self.real = None

    def __del__(self):
        self.close()

    def __enter__(self):
        yield self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def is_open(self):
        return self.real is not None and self.real.is_open

    def open(self):
        assert not self.is_open
        if not issubclass(self.__class__, LazyBase):
            raise NotImplementedError

    def close(self):
        if self.real is not None:
            if not (self.real.is_closing or self.real.is_closed):
                self.real.close()
                logger.info('%r closed', self)


class LazyConnection(LazyBase):

    def __init__(self,
                 host='localhost', port=5672, user='guest', password='guest'):
        super(LazyConnection, self).__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def open(self):
        super(LazyConnection, self).open()
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host, port=self.port, credentials=credentials)
        self.real = pika.BlockingConnection(parameters)
        logger.info('Connected to RabbitMQ')

    def channel(self):
        return LazyChannel(self)


class LazyChannel(LazyBase):

    def __init__(self, connection):
        super(LazyChannel, self).__init__()
        self.connection = connection

    def __getattr__(self, item):
        if not self.is_open:
            self.open()
        return getattr(self.real, item)

    def open(self):
        super(LazyChannel, self).open()
        if not self.connection.is_open:
            self.connection.open()

        self.real = self.connection.real.channel()
        logger.info('Connected to channel')
