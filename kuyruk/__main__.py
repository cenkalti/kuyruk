"""
This is the entry point for main "kuyruk" executable command.
It implements the command line parsing for subcommands and configuration.

"""
from __future__ import absolute_import
import os
import logging
import argparse

from kuyruk import __version__, importer, Kuyruk
from kuyruk.master import Master
from kuyruk.config import Config
from kuyruk.requeue import Requeuer
from kuyruk.manager import Manager
from kuyruk.scheduler import Scheduler


logger = logging.getLogger(__name__)


def run_worker(kuyruk, args):
    worker_class = importer.import_class_str(kuyruk.config.WORKER_CLASS)
    w = worker_class(kuyruk, args.queue)
    w.run()


def run_master(kuyruk, args):
    m = Master(kuyruk)
    m.run()


def run_requeue(kuyruk, args):
    r = Requeuer(kuyruk)
    r.run()


def run_manager(kuyruk, args):
    m = Manager(kuyruk)
    m.run()


def run_scheduler(kuyruk, args):
    s = Scheduler(kuyruk)
    s.run()


def main():
    parser = argparse.ArgumentParser(conflict_handler='resolve')

    # Add common options
    parser.add_argument(
        '-v', '--version', action='version', version=__version__)
    parser.add_argument(
        '-c', '--config',
        help='Python file containing Kuyruk configuration parameters')
    add_config_options(parser)

    subparsers = parser.add_subparsers(help='sub-command name')

    # Parser for the "worker" sub-command
    parser_worker = subparsers.add_parser('worker', help='run a worker')
    parser_worker.set_defaults(func=run_worker)
    parser_worker.add_argument(
        '-q', '--queue', default='kuyruk', help='consume tasks from')

    # Parser for the "master" sub-command
    parser_master = subparsers.add_parser('master', help='run master')
    parser_master.set_defaults(func=run_master)
    parser_master.add_argument(
        '-q', '--queues', help='comma seperated list of queues')

    # Parser for the "requeue" sub-command
    parser_master = subparsers.add_parser('requeue',
                                          help='requeue failed tasks')
    parser_master.set_defaults(func=run_requeue)

    # Parser for the "manager" sub-command
    parser_master = subparsers.add_parser('manager', help='run manager')
    parser_master.set_defaults(func=run_manager)

    # Parser for the "scheduler" sub-command
    parser_scheduler = subparsers.add_parser('scheduler', help='run scheduler')
    parser_scheduler.set_defaults(func=run_scheduler)

    # Parse arguments
    args = parser.parse_args()
    config = create_config(args)
    kuyruk = Kuyruk(config)

    # Run the sub-command function
    args.func(kuyruk, args)


def add_config_options(parser):
    """Adds options for overriding values in config."""
    config_group = parser.add_argument_group('override values in config')

    def to_option(attr):
        """Convert config key to command line option."""
        return '--%s' % attr.lower().replace('_', '-')

    # Add every attribute in Config as command line option
    for key in sorted(dir(Config)):
        if key.isupper():
            config_group.add_argument(to_option(key))


def create_config(args):
    """Creates Config object and overrides it's values from args."""
    config = Config()

    if args.config:
        # Load config file from command line option
        config.from_pyfile(args.config)
    else:
        # Load config file from environment variable
        env_config = os.environ.get('KUYRUK_CONFIG')
        if env_config:
            assert os.path.isabs(env_config)
            config.from_pyfile(env_config)

    config.from_env_vars()
    config.from_cmd_args(args)
    return config


if __name__ == '__main__':
    main()
