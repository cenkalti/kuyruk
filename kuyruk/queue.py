import logging
import socket
import pickle

import pika

from kuyruk.helpers import retry

logger = logging.getLogger(__name__)

# connection and channel is shared among all queues
class Queue(object):

    def __init__(self, name):

        self.credentials = pika.PlainCredentials(config.RABBIT_USER,
                                                 config.RABBIT_PASSWORD)
        self.parameters = pika.ConnectionParameters(host=config.RABBIT_HOST,
                                                    port=int(config.RABBIT_PORT),
                                                    credentials=self.credentials)
        self._connect()
        self._declare_queue()
    
    def __len__(self):
        return self._declare_queue().method.message_count

    def _connect(self):
        if not self.is_connected:
            # Connect to RabbitMQ
            connection = pika.BlockingConnection(self.parameters)
            logger.info('Connected to RabbitMQ')

            # Open the channel
            channel = connection.channel()
            logger.info('Created channel')

    def close(self):
        return  # newer allow the connection to be closed since it's shared among queues
        logger.info('Connection closed')
        self.connection.close()

    def _declare_queue(self):
        return self.channel.queue_declare(queue=self.name, durable=True,
                              exclusive=False, auto_delete=False)
    
    @property
    def is_connected(self):
        if self.connection:
            return self.connection.is_open

    def delete(self):
        try:
            self.channel.queue_delete(queue=self.name)
        except Exception as e:
            if e[0] != 404: # do not raise exceptions if queue is not found
                raise e
                
    def ack(self, delivery_tag=None):
        """
        Sends ack for given delivery tag.
        If no tag is given, sends ack for last message received with receive_single()

        """
        if delivery_tag:
            self.channel.basic_ack(delivery_tag=delivery_tag)
        else:
            self.channel.basic_ack(delivery_tag=self.last_delivery_tag)
                
    def reject(self):
        self.channel.basic_reject(delivery_tag=self.last_delivery_tag, requeue=True)
                
    def discard(self):
        self.channel.basic_reject(delivery_tag=self.last_delivery_tag, requeue=False)

    def receive_single(self):
        self._declare_queue()
        method_frame, header_frame, body = self.channel.basic_get(queue=self.name)
        self.last_delivery_tag = method_frame.delivery_tag if body else None
        #logger.info('Message received in queue: %s message: %s', (self.name, body))
        return body

    def send_raw(self, msg, content_type='text/plain'):
        self._declare_queue()
        logger.info('sending to queue: %s message: %s' % (self.name, msg))
        self.channel.basic_publish(exchange='',
                               routing_key=self.name,
                               body=msg,
                               properties=pika.BasicProperties(
                               content_type=content_type,
                               delivery_mode=2))

    @retry(Exception, tries=10, delay=1, backoff=1, logger=logger)
    def send(self, msg):
        '''coverts msg to json before sending'''
        self.send_raw(json.dumps(msg), 'application/json')
