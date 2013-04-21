import json
import socket
import pickle
import logging
from time import sleep
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
        self.canceling = False

        if self.local:
            self.name = "%s_%s" % (self.name, socket.gethostname())

    def __len__(self):
        return self.declare().method.message_count

    def declare(self):
        logger.warning('Declaring queue: %s', self.name)
        return self.channel.queue_declare(
            queue=self.name, durable=True,
            exclusive=False, auto_delete=False)

    @require_declare
    def receive(self):
        """Get a single message from queue."""
        message = self.channel.basic_get(self.name)
        return self._decode(message)

    @require_declare
    def send(self, obj):
        """Send a single message to the queue. obj must be JSON serializable."""
        logger.info('sending to queue: %s message: %r', self.name, obj)
        properties = pika.BasicProperties(
            content_type='application/json',
            delivery_mode=2)
        return self.channel.basic_publish(
            exchange='',
            routing_key=self.name,
            body=json.dumps(obj),
            properties=properties)

    def __iter__(self):
        self.generator = self.channel.consume(self.name)
        return self

    def next(self):
        if self.canceling:
            self.canceling = False
            raise StopIteration
        else:
            try:
                message = next(self.generator)
            except Exception as e:
                if e.args[0] == 4:  # Interrupted system call
                    raise StopIteration
                raise
            return self._decode(message)

    def cancel(self):
        self.canceling = True
        return self.channel.cancel()

    def pause(self, seconds):
        logger.info('Pausing')
        channel = self.channel
        channel.basic_cancel(channel._generator)
        sleep(seconds)
        channel._generator = channel.basic_consume(
            channel._generator_callback, self.name)
        logger.info('Resuming')

    def ack(self, delivery_tag):
        return self.channel.basic_ack(delivery_tag=delivery_tag)

    def reject(self, delivery_tag):
        """Reject the message. Message will be delivered to another worker."""
        return self.channel.basic_reject(
            delivery_tag=delivery_tag, requeue=True)

    def discard(self, delivery_tag):
        """Discard the message. Discarded messages will be lost."""
        return self.channel.basic_reject(
            delivery_tag=delivery_tag, requeue=False)

    def recover(self):
        return self.channel.basic_recover(requeue=True)

    def delete(self):
        try:
            return self.channel.queue_delete(queue=self.name)
        except pika.exceptions.ChannelClosed as e:
            # do not raise exceptions if queue is not found
            if e.args[0] != 404:
                raise

    def _decode(self, message):
        method, properties, body = message
        if body is None:
            return None, None

        if properties.content_type == 'application/json':
            obj = json.loads(body)
        elif properties.content_type == 'application/python-pickle':
            obj = pickle.loads(body)
        else:
            raise ValueError('Unknown content type')

        logger.debug(
            'Message received in queue: %s message: %s', self.name, obj)
        return method.delivery_tag, obj
