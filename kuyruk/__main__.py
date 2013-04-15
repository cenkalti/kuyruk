import imp
import logging
import optparse

from kuyruk import Kuyruk

logger = logging.getLogger(__name__)


def configure_logging():
    logging.getLogger('pika').level = logging.WARNING
    logging.basicConfig(level=logging.DEBUG)


def main():
    configure_logging()

    parser = optparse.OptionParser()
    parser.add_option('--config')
    # These options below override the options from config module
    parser.add_option('--queues')
    parser.add_option('--max-run-time', type='int')
    parser.add_option('--max-load', type='int')
    parser.add_option('--save-failed-tasks', action='store_true')
    options, args = parser.parse_args()

    if options.config is not None:
        config = imp.load_source('config', options.config)
    else:
        config = imp.new_module('config')

    if options.max_run_time is not None:
        config.KUYRUK_MAX_RUN_TIME = options.max_run_time

    if options.max_load is not None:
        config.KUYRUK_MAX_LOAD = options.max_load

    if options.save_failed_tasks is not None:
        config.KUYRUK_SAVE_FAILED_TASKS = options.save_failed_tasks

    kuyruk = Kuyruk(config_object=config)
    kuyruk.run(queues=options.queues)

if __name__ == '__main__':
    main()
