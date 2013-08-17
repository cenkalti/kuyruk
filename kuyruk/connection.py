import logging
from threading import RLock
from collections import defaultdict

from pika.adapters.blocking_connection import (BlockingConnection,
                                               BlockingChannel)

logger = logging.getLogger(__name__)


class Connection(BlockingConnection):

    def __init__(self, parameters=None):
        self._lock = RLock()
        super(Connection, self).__init__(parameters)

    def channel(self, channel_number=None):
        """Create a new channel with the next available or specified channel #.

        :param int channel_number: Specify the channel number

        """
        self._channel_open = False
        if not channel_number:
            channel_number = self._next_channel_number()
        logger.debug('Opening channel %i', channel_number)
        self._channels[channel_number] = Channel(self, channel_number)
        return self._channels[channel_number]

    def process_data_events(self):
        with self._lock:
            return super(Connection, self).process_data_events()

    def send_method(self, channel_number, method_frame, content=None):
        with self._lock:
            super(Connection, self).send_method(channel_number, method_frame,
                                                content)


class Channel(BlockingChannel):
    """Remembers the queues decalared and does not redeclare them."""

    SKIP_REDECLARE_QUEUE = True

    def __init__(self, connection, channel_number):
        super(Channel, self).__init__(connection, channel_number)
        self.declared = defaultdict(bool)

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None, force=False):
        if self.SKIP_REDECLARE_QUEUE and self.declared[queue] and not force:
            logger.debug("Queue is already declared, skipped declare.")
        else:
            rv = super(Channel, self).queue_declare(
                queue, passive, durable, exclusive,
                auto_delete, nowait, arguments)
            self.declared[queue] = True
            return rv
