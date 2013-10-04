from __future__ import absolute_import
import socket
import logging

import pika

from kuyruk.message import Message


logger = logging.getLogger(__name__)


class Queue(object):

    def __init__(self, name, channel, local=False):
        if channel is None:
            from kuyruk import Kuyruk
            channel = Kuyruk().channel()

        self.name = name
        self.channel = channel
        self.local = local
        self.canceling = False

        if self.local:
            self.name = "%s.%s" % (self.name, socket.gethostname())

        self.declare()

    def __len__(self):
        return self.declare(force=True).method.message_count

    def declare(self, force=False):
        logger.debug('Declaring queue: %s', self.name)
        return self.channel.queue_declare(
            queue=self.name, durable=True,
            exclusive=False, auto_delete=False, force=force)

    def receive(self):
        """Get a single message from queue."""
        message = self.channel.basic_get(self.name)
        return Message.decode(message)

    def send(self, obj, expiration=None):
        """Send a single message to the queue.
        obj must be JSON serializable."""
        logger.info('sending to queue: %r message: %r', self.name, obj)
        properties = pika.BasicProperties(
            content_type='application/json',
            expiration=expiration,
            delivery_mode=2)
        return self.channel.basic_publish(
            exchange='',
            routing_key=self.name,
            body=Message.encode(obj),
            properties=properties)

    def ack(self, delivery_tag):
        logger.debug('Acking message')
        return self.channel.basic_ack(delivery_tag=delivery_tag)

    def nack(self, delivery_tag, multiple=False, requeue=True):
        logger.debug('Nacking message')
        return self.channel.basic_nack(
            delivery_tag=delivery_tag, multiple=multiple, requeue=requeue)

    def reject(self, delivery_tag):
        """Reject the message. Message will be delivered to another worker."""
        logger.debug('Rejecting message')
        return self.channel.basic_reject(
            delivery_tag=delivery_tag, requeue=True)

    def discard(self, delivery_tag):
        """Discard the message. Discarded messages will be lost."""
        logger.debug('Discarding message')
        return self.channel.basic_reject(
            delivery_tag=delivery_tag, requeue=False)

    def recover(self):
        logger.debug('Recovering messages')
        return self.channel.basic_recover(requeue=True)

    def delete(self):
        logger.warning('Deleting queue')
        return self.channel.queue_delete(queue=self.name)

    def basic_consume(self, callback):
        logger.debug('Issuing Basic.Consume')
        return self.channel.basic_consume(callback, self.name)

    def basic_cancel(self, consumer_id):
        logger.debug('Issuing Basic.Cancel')
        return self.channel.basic_cancel(consumer_id)

    def basic_qos(self, *args, **kwargs):
        return self.channel.basic_qos(*args, **kwargs)

    def tx_select(self):
        return self.channel.tx_select()

    def tx_commit(self):
        return self.channel.tx_commit()
