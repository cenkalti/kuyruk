from __future__ import absolute_import
import logging
import threading
from Queue import Queue, Empty
from contextlib import contextmanager

from kuyruk.helpers import queue_get_all, start_daemon_thread
from kuyruk.message import Message

logger = logging.getLogger(__name__)


class Consumer(object):

    def __init__(self, queue):
        self.queue = queue
        self._generator = None
        self._generator_messages = Queue()
        self._stop_processing_data_events = threading.Event()
        self._message_iterator = None
        self.consuming = False

    @contextmanager
    def consume(self):
        """Blocking consumption of a queue."""
        # Issue Basic.Consume
        self._consume()

        # Start data events thread
        self._stop_processing_data_events.clear()
        start_daemon_thread(self._process_data_events)

        # Return message iterator
        self._message_iterator = MessageIterator(
            self._generator_messages, self.queue)
        yield self._message_iterator

        # Issue Basic.Cancel
        self._cancel()

        # Stop data events thread
        self._stop_processing_data_events.set()

    def stop(self):
        """Stop consumption of messages."""
        if self._message_iterator:
            self._message_iterator.stop()

    def pause(self, seconds):
        logger.info('Pausing for %i seconds...', seconds)
        self._cancel()
        self.queue.channel.connection.sleep(seconds)
        logger.info('Resuming')
        self._consume()

    def _consume(self):
        logger.debug('Start consuming')
        assert self._generator is None
        self._generator = self.queue.basic_consume(self._generator_callback)
        self.consuming = True

    def _generator_callback(self, unused, method, properties, body):
        """Called when a message is received from RabbitMQ and appended to the
        list of messages to be returned when a message is received by RabbitMQ.

        """
        logger.debug('Adding a message to generator messages')
        self._generator_messages.put((method, properties, body))

    def _cancel(self):
        """Cancel the consumption of a queue, rejecting all pending messages.

        """
        logger.debug('_cancel is called')
        count_messages = 0
        self.queue.basic_cancel(self._generator)
        if not self._generator_messages.empty():
            logger.debug('There are message pending, nacking all')
            remaining_messages = queue_get_all(self._generator_messages)
            last_message = remaining_messages[-1]
            method, properties, body = last_message
            count_messages = len(remaining_messages)
            logger.info('Requeueing %i messages with delivery tag %s',
                        count_messages, method.delivery_tag)
            self.queue.nack(method.delivery_tag, multiple=True, requeue=True)

        self._generator = None
        self.consuming = False
        return count_messages

    def _process_data_events(self):
        while not self._stop_processing_data_events.is_set():
            self.queue.channel.connection.process_data_events()


class MessageIterator(object):

    def __init__(self, messages, queue):
        self.messages = messages
        self.queue = queue
        self._stop = threading.Event()

    def __iter__(self):
        return self

    def next(self):
        logger.info('Waiting for new message...')
        while not self._stop.is_set():
            try:
                message = self.messages.get(timeout=0.1)
            except Empty:
                pass
            else:
                return Message(message, self.queue)

        logger.debug("Exiting from iterator")
        raise StopIteration

    def stop(self):
        self._stop.set()
