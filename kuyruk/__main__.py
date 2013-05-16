from __future__ import absolute_import
import ast
import logging
import argparse
from kuyruk import __version__, requeue, manager
from kuyruk.worker import Worker
from kuyruk.master import Master
from kuyruk.config import Config

logger = logging.getLogger(__name__)


def worker(config, args):
    w = Worker(config, args.queue)
    w.run()


def master(config, args):
    m = Master(config)
    m.run()


def main():
    parser = argparse.ArgumentParser()

    # Add common options
    parser.add_argument(
        '-v', '--version', action='version', version=__version__)
    parser.add_argument(
        '--config',
        help='Python file containing Kuyruk configuration parameters')
    add_config_options(parser)

    subparsers = parser.add_subparsers(help='sub-command name')

    # Parser for the "worker" sub-command
    parser_worker = subparsers.add_parser('worker', help='run a worker')
    parser_worker.set_defaults(func=worker)
    parser_worker.add_argument(
        '--queue', default='kuyruk', help='consume tasks from')

    # Parser for the "master" sub-command
    parser_master = subparsers.add_parser('master', help='run a master')
    parser_master.set_defaults(func=master)

    # Parser for the "requeue" sub-command
    parser_master = subparsers.add_parser('requeue', help='requeue failed tasks')
    parser_master.set_defaults(func=requeue.run)

    # Parser for the "manager" sub-command
    parser_master = subparsers.add_parser('manager', help='run manager')
    parser_master.set_defaults(func=manager.run)

    # Parse arguments
    args = parser.parse_args()
    config = create_config(args)

    # Run the sub-command function
    args.func(config, args)


def add_config_options(parser):
    """Adds options for overriding values in config."""
    config_group = parser.add_argument_group('override values in config')

    # Add every attribute in Config as command line option
    for key in sorted(dir(Config), reverse=True):
        if key.isupper():
            config_group.add_argument(to_option(key))


def create_config(args):
    """Creates Config object and overrides it's values from args."""
    config = Config()
    if args.config:
        config.from_pyfile(args.config)

    # Override values in config
    for key, value in vars(args).iteritems():
        if value is not None:
            key = to_attr(key)
            if hasattr(Config, key):
                try:
                    value = ast.literal_eval(value)
                except ValueError:
                    pass
                setattr(config, key, value)

    return config


def to_option(attr):
    return '--%s' % attr.lower().replace('_', '-')


def to_attr(option):
    return option.upper().replace('-', '_')


if __name__ == '__main__':
    main()
