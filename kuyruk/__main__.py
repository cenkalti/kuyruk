import imp
import logging
import optparse

from kuyruk import Kuyruk


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('pika').level = logging.WARNING

    parser = optparse.OptionParser()
    parser.add_option('-c', '--config')
    parser.add_option('-r', '--max-run-time', type='int')
    options, args = parser.parse_args()

    if not args:
        args = ['kuyruk']

    if options.config:
        config = imp.load_source('config', options.config)
    else:
        config = imp.new_module('config')

    if options.max_run_time:
        config.KUYRUK_MAX_RUN_TIME = options.max_run_time

    kuyruk = Kuyruk(config=config)
    kuyruk.run(args[0])

if __name__ == '__main__':
    main()
