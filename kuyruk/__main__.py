from __future__ import absolute_import
import sys
import logging
import logging.config
import optparse

from kuyruk import __version__
from kuyruk.kuyruk import Kuyruk
from kuyruk.config import Config

logger = logging.getLogger(__name__)


def main():
    parser = optparse.OptionParser()
    parser.add_option('--version', action='store_true')
    parser.add_option('--config')
    parser.add_option('--logging-config')
    parser.add_option('--logging-level', default='INFO')
    # These options below override the options from config module
    parser.add_option('--queues')
    parser.add_option('--max-load', type='float')
    parser.add_option('--max-run-time', type='float')
    parser.add_option('--save-failed-tasks', action='store_true')
    options, args = parser.parse_args()

    if options.version:
        print __version__
        sys.exit()

    if options.logging_config:
        logging.config.fileConfig(options.logging_config)
    else:
        logging.getLogger('pika').level = logging.WARNING
        level = getattr(logging, options.logging_level)
        logging.basicConfig(level=level)

    config = Config(options.config)

    if options.max_load is not None:
        config.MAX_LOAD = options.max_load

    if options.max_run_time is not None:
        config.MAX_RUN_TIME = options.max_run_time

    if options.save_failed_tasks:
        config.SAVE_FAILED_TASKS = options.save_failed_tasks

    kuyruk = Kuyruk()
    kuyruk.config = config
    kuyruk.run(options.queues)

if __name__ == '__main__':
    main()
