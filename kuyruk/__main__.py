from __future__ import absolute_import
import logging
from kuyruk.kuyruk import Kuyruk
from kuyruk.optionparser import OptionParser

logger = logging.getLogger(__name__)


def main():
    parser = OptionParser()

    # These options below override the options from config module
    parser.add_option(
        '--queues',
        help="comma seperated list of queues that the workers will run on")
    parser.add_option('--manager-host')
    parser.add_option('--manager-port', type='int')
    parser.add_option('--max-load', type='float')
    parser.add_option('--max-run-time', type='float')
    parser.add_option('--save-failed-tasks', action='store_true')
    parser.add_option('--no-save-failed-tasks', action='store_true')

    options, args = parser.parse_args()
    config = parser.config

    if options.manager_host:
        config.MANAGER_HOST = options.manager_host

    if options.manager_port:
        config.MANAGER_PORT = options.manager_port

    if options.max_load:
        config.MAX_LOAD = options.max_load

    if options.max_run_time:
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
