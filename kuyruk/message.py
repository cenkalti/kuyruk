import json
import pickle
import logging

from .helpers.json_datetime import JSONEncoder, JSONDecoder

logger = logging.getLogger(__name__)


class Message(object):

    def __init__(self, message, queue):
        """
        :param message: return value of pika.Channel.basic_get
        :param queue: kuyruk.queue.Queue instance

        """
        self.queue = queue
        self.method = message[0]
        self.properties = message[1]
        self.body = message[2]

    def ack(self):
        self.queue.ack(self.method.delivery_tag)

    def reject(self):
        self.queue.reject(self.method.delivery_tag)

    def discard(self):
        self.queue.discard(self.method.delivery_tag)

    def get_object(self):
        """Decode and return wrapped message body."""
        return self.decode((self.method, self.properties, self.body))[1]

    @staticmethod
    def encode(obj):
        return json.dumps(obj, cls=JSONEncoder)

    @staticmethod
    def decode(message):
        method, properties, body = message
        if body is None:
            return None, None

        if properties.content_type == 'application/json':
            obj = json.loads(body, cls=JSONDecoder)
        elif properties.content_type == 'application/python-pickle':
            obj = pickle.loads(body)
        else:
            raise TypeError('Unknown content type')

        logger.debug('Message decoded: %s', obj)
        return method.delivery_tag, obj
