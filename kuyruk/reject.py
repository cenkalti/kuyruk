import logging
import collections

from monotonic import monotonic

logger = logging.getLogger(__name__)

Reject = collections.namedtuple('Reject', 'send_time, delivery_tag, requeue')


class DelayedRejects(object):

    def __init__(self, channel):
        self._channel = channel
        self._queue = collections.deque()
        self._prefetch_count = 1

    def push(self, delay, delivery_tag, requeue=False):
        reject = Reject(monotonic() + delay, delivery_tag, requeue)
        self._queue.append(reject)
        self._change_prefetch_count(1)

    def send_pending(self):
        while self._queue:
            front = self._queue[0]
            if monotonic() < front.send_time:
                break

            logger.debug('Sending delayed reject: %s', front)
            self._change_prefetch_count(-1)
            self._channel.basic_reject(front.delivery_tag, front.requeue)
            self._queue.popleft()

    def _change_prefetch_count(self, incr):
        self._prefetch_count += incr
        self._channel.basic_qos(0, self._prefetch_count, True)
