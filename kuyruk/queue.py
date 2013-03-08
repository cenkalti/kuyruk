import logging
import socket
import pickle

import pika

from kuyruk.helpers import retry

logger = logging.getLogger(__name__)


class Queue(object):

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection
        self._channel = None

    @property
    def channel(self):
        if self._channel is None:
            self._channel = self.connection.channel()

        self._channel.queue_declare(queue=self.name, durable=True,
                                   exclusive=False, auto_delete=False)
        return self._channel

    def __len__(self):
        return self.channel.method.message_count

    def close(self):
        self.channel.close()
        logger.info('Channel closed')

    def delete(self):
        try:
            self.channel.queue_delete(queue=self.name)
        except Exception as e:
            # do not raise exceptions if queue is not found
            if e[0] != 404:
                raise
                
    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def reject(self, delivery_tag):
        self.channel.basic_reject(delivery_tag=delivery_tag, requeue=True)
                
    def discard(self, delivery_tag):
        self.channel.basic_reject(delivery_tag=delivery_tag, requeue=False)

    def receive(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.name)
        obj = pickle.loads(body)
        logger.debug('Message received in queue: %s message: %s', self.name, obj)
        return method_frame.delivery_tag, obj

    def send(self, obj):
        logger.info('sending to queue: %s message: %r', self.name, obj)
        properties = pika.BasicProperties(
            content_type='application/python-pickle',
            delivery_mode=2)
        self.channel.basic_publish(
            exchange='',
            routing_key=self.name,
            body=pickle.dumps(obj),
            properties=properties)
