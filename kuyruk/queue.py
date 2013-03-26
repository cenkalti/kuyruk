import socket
import pickle
import logging
from functools import wraps

import pika

logger = logging.getLogger(__name__)


def require_declare(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        if not self.declared:
            self.channel.queue_declare(
                queue=self.name, durable=True,
                exclusive=False, auto_delete=False)
            self.declared = True
        return f(self, *args, **kwargs)
    return inner


class Queue(object):

    def __init__(self, name, channel, local=False):
        self.name = name
        self.channel = channel
        self.local = local
        self.declared = False

        if self.local:
            self.name = "%s_%s" % (self.name, socket.gethostname())

    @require_declare
    def delete(self):
        try:
            self.channel.queue_delete(queue=self.name)
        except Exception as e:
            # do not raise exceptions if queue is not found
            if e[0] != 404:
                raise

    @require_declare
    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    @require_declare
    def reject(self, delivery_tag):
        self.channel.basic_reject(delivery_tag=delivery_tag, requeue=True)

    @require_declare
    def discard(self, delivery_tag):
        self.channel.basic_reject(delivery_tag=delivery_tag, requeue=False)

    @require_declare
    def receive(self):
        method_frame, header_frame, body = self.channel.basic_get(self.name)
        if body is None:
            return None

        obj = pickle.loads(body)
        logger.debug(
            'Message received in queue: %s message: %s', self.name, obj)
        return method_frame.delivery_tag, obj

    @require_declare
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
