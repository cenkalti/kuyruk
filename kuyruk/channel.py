import logging

import pika

logger = logging.getLogger(__name__)


class LazyChannel(object):

    def __init__(self, host='localhost', port=5672,
                 user='guest', password='guest'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection = None
        self.channel = None

    def __getattr__(self, item):
        if not self.is_open:
            self.open()
        return getattr(self.channel, item)

    def __del__(self):
        if self.is_open:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def is_open(self):
        if self.channel is None:
            return False
        return self.channel.is_open

    def open(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            heartbeat_interval=600,
            socket_timeout=2,
            connection_attempts=2)
        self.connection = pika.BlockingConnection(parameters)
        logger.info('Connected to RabbitMQ')
        self.channel = self.connection.channel()
        logger.debug('Opened channel')

    def close(self):
        if self.is_open:
            self.channel.close()
            self.connection.close()
            logger.info('%r closed', self)
