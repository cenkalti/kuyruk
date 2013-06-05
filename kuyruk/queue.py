from __future__ import absolute_import
import os
import errno
import socket
import logging
import traceback
from functools import wraps
from threading import RLock

import pika
import pika.exceptions

from kuyruk.message import Message
from kuyruk.helpers import synchronized

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
        if channel is None:
            from kuyruk import Kuyruk
            channel = Kuyruk()._channel()

        self.name = name
        self.channel = channel
        self.local = local
        self.declared = False
        self.canceling = False
        self.lock = RLock()

        if self.local:
            self.name = "%s.%s" % (self.name, socket.gethostname())

    @synchronized
    def __len__(self):
        return self.declare().method.message_count

    @synchronized
    def declare(self):
        logger.debug('Declaring queue: %s', self.name)
        return self.channel.queue_declare(
            queue=self.name, durable=True,
            exclusive=False, auto_delete=False)

    @synchronized
    @require_declare
    def receive(self):
        """Get a single message from queue."""
        message = self.channel.basic_get(self.name)
        return Message.decode(message)

    @synchronized
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
            body=Message.encode(obj),
            properties=properties)

    @synchronized
    def ack(self, delivery_tag):
        logger.debug('Acking message')
        return self.channel.basic_ack(delivery_tag=delivery_tag)

    @synchronized
    def nack(self, delivery_tag, multiple=False, requeue=True):
        logger.debug('Nacking message')
        return self.channel.basic_nack(
            delivery_tag=delivery_tag, multiple=multiple, requeue=requeue)

    @synchronized
    def reject(self, delivery_tag):
        """Reject the message. Message will be delivered to another worker."""
        logger.debug('Rejecting message')
        return self.channel.basic_reject(
            delivery_tag=delivery_tag, requeue=True)

    @synchronized
    def discard(self, delivery_tag):
        """Discard the message. Discarded messages will be lost."""
        logger.debug('Discarding message')
        return self.channel.basic_reject(
            delivery_tag=delivery_tag, requeue=False)

    @synchronized
    def recover(self):
        logger.debug('Recovering messages')
        return self.channel.basic_recover(requeue=True)

    @synchronized
    def delete(self):
        """Deletes queue. Does not raise exception if queue is not found."""
        logger.warning('Deleting queue')
        try:
            return self.channel.queue_delete(queue=self.name)
        except pika.exceptions.ChannelClosed as e:
            # do not raise exceptions if queue is not found
            if e.args[0] != 404:
                raise

    @synchronized
    def basic_consume(self, callback):
        logger.debug('Issuing Basic.Consume')
        return self.channel.basic_consume(callback, self.name)

    @synchronized
    def basic_cancel(self, consumer_id):
        logger.debug('Issuing Basic.Cancel')
        return self.channel.basic_cancel(consumer_id)

    @synchronized
    def process_data_events(self):
        try:
            self.channel.connection.process_data_events()
        except Exception as e:
            logger.debug(e)
            if e.args[0] == errno.EINTR:  # Interrupted system call
                # Happens when a signal is received. No harm.
                pass
            else:
                logger.critical(traceback.format_exc())
                os._exit(1)

    @synchronized
    def sleep(self, seconds):
        self.channel.connection.sleep(seconds)

    @synchronized
    def basic_qos(self, *args, **kwargs):
        return self.channel.basic_qos(*args, **kwargs)

    @synchronized
    def tx_select(self):
        return self.channel.tx_select()

    @synchronized
    def tx_commit(self):
        return self.channel.tx_commit()
