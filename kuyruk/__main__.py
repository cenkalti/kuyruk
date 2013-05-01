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
    parser = optparse.OptionParser(version=__version__)
    parser.add_option('-c', '--config')
    parser.add_option(
        '-l', '--logging-level', default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    parser.add_option(
        '--logging-config',
        help="INI style logging configuration file")

    # These options below override the options from config module
    parser.add_option(
        '--queues',
        help="Comma seperated list of queues that the workers will run on")
    parser.add_option('--max-load', type='float')
    parser.add_option('--max-run-time', type='float')
    parser.add_option('--save-failed-tasks', action='store_true')
    parser.add_option('--no-save-failed-tasks', action='store_true')
    options, args = parser.parse_args()

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
        config.SAVE_FAILED_TASKS = True
    elif options.no_save_failed_tasks:
        config.SAVE_FAILED_TASKS = False

    kuyruk = Kuyruk()
    kuyruk.config = config
    kuyruk.run(options.queues)

if __name__ == '__main__':
    main()
