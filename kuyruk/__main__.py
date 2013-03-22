import os
import imp
import time
import logging
import optparse

from kuyruk import Kuyruk

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('pika').level = logging.WARNING

    parser = optparse.OptionParser()
    parser.add_option('-c', '--config')
    # These options below override the options from config module
    parser.add_option('-q', '--queue')
    parser.add_option('-r', '--max-run-time', type='int')
    parser.add_option('-t', '--max-tasks', type='int')
    options, args = parser.parse_args()

    if options.config:
        config = imp.load_source('config', options.config)
    else:
        config = imp.new_module('config')

    if options.queue is not None:
        config.KUYRUK_QUEUE = options.queue

    if options.max_run_time is not None:
        config.KUYRUK_MAX_RUN_TIME = options.max_run_time

    if options.max_tasks is not None:
        config.KUYRUK_MAX_TASKS = options.max_tasks

    kuyruk = Kuyruk(config=config)
    kuyruk.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning("Warm shutdown")
        kuyruk.stop()
        try:
            while kuyruk.isAlive():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.critical('Cold shutdown!')
            os._exit(1)

if __name__ == '__main__':
    main()
