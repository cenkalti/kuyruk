from __future__ import absolute_import
import os
import ast
import logging
import argparse
from kuyruk import __version__, importer
from kuyruk.kuyruk import Kuyruk
from kuyruk.master import Master
from kuyruk.config import Config
from kuyruk.requeue import Requeuer
from kuyruk.manager import Manager

logger = logging.getLogger(__name__)


def worker(kuyruk, args):
    worker_class = importer.import_class_str(kuyruk.config.WORKER_CLASS)
    w = worker_class(kuyruk, args.queue)
    w.run()


def master(kuyruk, args):
    m = Master(kuyruk)
    m.run()


def requeue(kuyruk, args):
    r = Requeuer(kuyruk)
    r.run()


def manager(kuyruk, args):
    m = Manager(kuyruk)
    m.run()


def main():
    parser = argparse.ArgumentParser(conflict_handler='resolve')

    # Add common options
    parser.add_argument(
        '-v', '--version', action='version', version=__version__)
    parser.add_argument(
        '-c', '--config',
        help='Python file containing Kuyruk configuration parameters')
    parser.add_argument(
        '--delete-config', action='store_true',
        help='delete config after loading (used internally)')
    add_config_options(parser)

    subparsers = parser.add_subparsers(help='sub-command name')

    # Parser for the "worker" sub-command
    parser_worker = subparsers.add_parser('worker', help='run a worker')
    parser_worker.set_defaults(func=worker)
    parser_worker.add_argument(
        '-q', '--queue', default='kuyruk', help='consume tasks from')

    # Parser for the "master" sub-command
    parser_master = subparsers.add_parser('master', help='run a master')
    parser_master.set_defaults(func=master)
    parser_master.add_argument(
        '-q', '--queues', help='comma seperated list of queues')

    # Parser for the "requeue" sub-command
    parser_master = subparsers.add_parser('requeue', help='requeue failed tasks')
    parser_master.set_defaults(func=requeue)

    # Parser for the "manager" sub-command
    parser_master = subparsers.add_parser('manager', help='run manager')
    parser_master.set_defaults(func=manager)

    # Parse arguments
    args = parser.parse_args()
    config = create_config(args)
    kuyruk = Kuyruk(config)

    # Run the sub-command function
    args.func(kuyruk, args)


def add_config_options(parser):
    """Adds options for overriding values in config."""
    config_group = parser.add_argument_group('override values in config')

    # Add every attribute in Config as command line option
    for key in sorted(dir(Config)):
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
                except (ValueError, SyntaxError):
                    pass
                setattr(config, key, value)

    if args.delete_config:
        os.unlink(args.config)

    return config


def to_option(attr):
    return '--%s' % attr.lower().replace('_', '-')


def to_attr(option):
    return option.upper().replace('-', '_')


if __name__ == '__main__':
    main()
