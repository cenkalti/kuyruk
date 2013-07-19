import sys
import logging
from collections import defaultdict
from threading import Lock

import pika.adapters.blocking_connection
from pika.adapters.blocking_connection import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

logger = logging.getLogger(__name__)


class Connection(BlockingConnection):

    # def channel(self, channel_number=None):
    #     pika.adapters.blocking_connection.BlockingChannel = RememberingChannel
    #     rv = super(Connection, self).channel(channel_number)
    #     pika.adapters.blocking_connection.BlockingChannel = BlockingChannel
    #     return rv

    def channel(self, channel_number=None):
        """Create a new channel with the next available or specified channel #.

        :param int channel_number: Specify the channel number

        """
        self._channel_open = False
        if not channel_number:
            channel_number = self._next_channel_number()
        logger.debug('Opening channel %i', channel_number)
        self._channels[channel_number] = RememberingChannel(self, channel_number)
        return self._channels[channel_number]


class RememberingChannel(BlockingChannel):
    """Remembers the queues decalared and does not redeclare them."""

    def __init__(self, connection, channel_number):
        super(RememberingChannel, self).__init__(connection, channel_number)
        self.declared = defaultdict(bool)

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        if not self.declared[queue] or 'nose' in sys.argv[0]:
            rv = super(RememberingChannel, self).queue_declare(
                queue, passive, durable, exclusive,
                auto_delete, nowait, arguments)
            self.declared[queue] = True
            return rv
        else:
            logger.debug("Queue is already declared, skipped declare.")
