from __future__ import absolute_import
import logging

from kuyruk.channel import LazyChannel
from kuyruk.queue import Queue
from kuyruk.optionparser import OptionParser

logger = logging.getLogger(__name__)


def main():
    parser = OptionParser()
    parser.parse_args()
    config = parser.config

    channel = LazyChannel.from_config(config)
    channel.tx_select()
    failed_queue = Queue('kuyruk_failed', channel)

    while 1:
        tag, task_description = failed_queue.receive()
        if task_description is None:
            break

        queue_name = task_description['queue']
        del task_description['queue']
        task_queue = Queue(queue_name, channel)
        task_queue.send(task_description)

        failed_queue.ack(tag)
        channel.tx_commit()

    logger.info('All failed task are requeued.')

if __name__ == '__main__':
    main()
