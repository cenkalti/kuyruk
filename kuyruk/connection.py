import time
import logging
from functools import wraps

import pika

logger = logging.getLogger(__name__)


def require_open(f):
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

    @property
    def is_open(self):
        return self.real is not None and self.real.is_open

    def open(self):
        assert not self.is_open
        if not issubclass(self.__class__, LazyBase):
            raise NotImplementedError

    def close(self):
        if self.is_open and not self.real.is_closing:
            self.real.close()
            logger.info('%r closed', self)


class LazyConnection(LazyBase):

    def __init__(self, host, port, user, password):
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

    @require_open
    def channel(self):
        return LazyChannel(self)


class LazyChannel(LazyBase):

    def __init__(self, connection):
        super(LazyChannel, self).__init__()
        self.connection = connection

    def open(self):
        super(LazyChannel, self).open()
        if not self.connection.is_open:
            self.connection.open()

        self.real = self.connection.real.channel()
        logger.info('Connected to channel')

    @require_open
    def queue_declare(self, queue, durable, exclusive, auto_delete):
        return self.real.queue_declare(
            queue=queue, durable=durable,
            exclusive=exclusive, auto_delete=auto_delete)

    @require_open
    def queue_delete(self, queue):
        self.real.queue_delete(queue=queue)

    @require_open
    def basic_ack(self, delivery_tag):
        self.real.basic_ack(delivery_tag=delivery_tag)

    @require_open
    def basic_reject(self, delivery_tag, requeue):
        self.real.basic_reject(delivery_tag=delivery_tag, requeue=requeue)

    @require_open
    def basic_get(self, queue):
        return self.real.basic_get(queue=queue)

    @require_open
    def basic_publish(self, exchange, routing_key, body, properties):
        self.real.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=properties)
