import logging
import optparse

from .channel import LazyChannel
from .queue import Queue
from .config import Config

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO)

    parser = optparse.OptionParser()
    parser.add_option('--config')
    options, args = parser.parse_args()

    config = Config(options.config)
    channel = LazyChannel(
        config.RABBIT_HOST, config.RABBIT_PORT,
        config.RABBIT_USER, config.RABBIT_PASSWORD)
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
