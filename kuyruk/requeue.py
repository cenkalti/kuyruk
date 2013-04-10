import imp
import logging
import optparse

from .__main__ import configure_logging
from .connection import LazyConnection
from .queue import Queue
from .config import Config

logger = logging.getLogger(__name__)


def main():
    configure_logging()

    parser = optparse.OptionParser()
    parser.add_option('--config')
    options, args = parser.parse_args()

    if options.config is not None:
        config = imp.load_source('config', options.config)
    else:
        config = imp.new_module('config')

    config = Config(config)
    connection = LazyConnection(
        config.RABBIT_HOST, config.RABBIT_PORT,
        config.RABBIT_USER, config.RABBIT_PASSWORD)
    channel = connection.channel()
    channel.tx_select()
    failed_queue = Queue('kuyruk_failed', channel)

    while 1:
        message = failed_queue.receive()
        if message is None:
            break

        tag, task_description = message
        queue_name = task_description['queue']
        del task_description['queue']
        task_queue = Queue(queue_name, channel)
        task_queue.send(task_description)

        failed_queue.ack(tag)
        channel.tx_commit()

    logger.info('All failed task are requeued.')

if __name__ == '__main__':
    main()
