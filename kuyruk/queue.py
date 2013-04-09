import socket
import pickle
import logging
from functools import wraps

import pika
import pika.exceptions

logger = logging.getLogger(__name__)


def require_declare(f):
    """Declare queue before running the wrapping function.
    Also, if queue is deleted while worker is running, declare again.

    """
    @wraps(f)
    def inner(self, *args, **kwargs):
        try:
            if not self.declared:
                # This declare() here is runs only once when the first function
                # is called on the queue. Queue needs to be declared before
                # doing anything with the queue.
                self.declare()
                self.declared = True
            return f(self, *args, **kwargs)
        except pika.exceptions.ChannelClosed as e:
            # If queue is not found, declare queue and try again
            # Queue migth be deleted while working on this queue.
            if e.args[0] == 404:
                logger.warning("Queue(%r) is not found", self.name)
                self.declare()
                return f(self, *args, **kwargs)
            raise
    return inner


class Queue(object):

    def __init__(self, name, channel, local=False):
        self.name = name
        self.channel = channel
        self.local = local
        self.declared = False

        if self.local:
            self.name = "%s_%s" % (self.name, socket.gethostname())

    def declare(self):
        logger.warning('Declaring queue: %s', self.name)
        self.channel.queue_declare(
            queue=self.name, durable=True,
            exclusive=False, auto_delete=False)

    def delete(self):
        try:
            self.channel.queue_delete(queue=self.name)
        except pika.exceptions.ChannelClosed as e:
            # do not raise exceptions if queue is not found
            if e.args[0] != 404:
                raise

    @require_declare
    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    @require_declare
    def reject(self, delivery_tag):
        """Reject the message. Message will be delivered to another worker."""
        self.channel.basic_reject(delivery_tag=delivery_tag, requeue=True)

    @require_declare
    def discard(self, delivery_tag):
        """Discard the message. Discarded messages will be lost."""
        self.channel.basic_reject(delivery_tag=delivery_tag, requeue=False)

    @require_declare
    def recover(self):
        self.channel.basic_recover(requeue=True)

    @require_declare
    def receive(self):
        """Get a single message from queue. If not any message is available
        returns None."""
        method_frame, header_frame, body = self.channel.basic_get(self.name)
        if body is None:
            return None

        obj = pickle.loads(body)
        logger.debug(
            'Message received in queue: %s message: %s', self.name, obj)
        return method_frame.delivery_tag, obj

    @require_declare
    def send(self, obj):
        """Send a single message to the queue. obj should be pickleable."""
        logger.info('sending to queue: %s message: %r', self.name, obj)
        properties = pika.BasicProperties(
            content_type='application/python-pickle',
            delivery_mode=2)
        self.channel.basic_publish(
            exchange='',
            routing_key=self.name,
            body=pickle.dumps(obj),
            properties=properties)
