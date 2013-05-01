from __future__ import absolute_import
import os
import logging
import threading
from Queue import Queue, Empty

from kuyruk.message import Message

logger = logging.getLogger(__name__)


class Consumer(object):

    def __init__(self, queue):
        self.queue = queue
        self._generator = None
        self._generator_messages = Queue()
        self._stop = threading.Event()

    def __iter__(self):
        """Blocking consumption of a queue instead of via a callback."""
        self._start()
        return self

    def stop(self):
        """Stop consumption of messages."""
        self._stop.set()

    def pause(self, seconds):
        logger.info('Pausing for %i seconds...', seconds)
        self._cancel()
        with self.queue.lock:
            self.queue.channel.connection.sleep(seconds)
        logger.info('Resuming')
        self._start()

    def _start(self):
        logger.debug('Start consuming')
        if not self._generator:
            self._generator = self.queue.basic_consume(self._generator_callback)

    def next(self):
        while not self._stop.is_set():
            try:
                message = self._generator_messages.get(timeout=0.1)
                return Message(message, self.queue)
            except Empty:
                pass

            try:
                with self.queue.lock:
                    self.queue.channel.connection.process_data_events()
            except Exception as e:
                logger.debug(e)
                if e.args[0] != 4:  # Interrupted system call
                    logger.critical("Connection is closed")
                    os._exit(1)

        logger.debug("Exiting from iterator")
        self._cancel()
        self._stop.clear()
        raise StopIteration

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
        remaining_messages = []
        count_messages = 0
        self.queue.basic_cancel(self._generator)
        if not self._generator_messages.empty():
            logger.debug('There are message pending, nacking all')
            # Get the last item
            try:
                while 1:
                    message = self._generator_messages.get_nowait()
                    remaining_messages.append(message)
            except Empty:
                pass
            last_message = remaining_messages[-1]
            method, properties, body = last_message
            count_messages = len(remaining_messages)
            logger.info('Requeueing %i messages with delivery tag %s',
                        count_messages, method.delivery_tag)
            self.queue.nack(method.delivery_tag, multiple=True, requeue=True)
            with self.queue.lock:
                self.queue.channel.connection.process_data_events()
        self._generator = None
        return count_messages
