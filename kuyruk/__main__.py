"""
This is the entry point for main "kuyruk" executable command.
It implements the command line parsing for subcommands and configuration.

"""
from __future__ import absolute_import
import os
import sys
import logging
import argparse

from kuyruk import __version__, importer
from kuyruk.config import Config


logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(conflict_handler='resolve')

    # Add common options
    parser.add_argument(
        '-v', '--version', action='version', version=__version__)
    parser.add_argument(
        '-c', '--config',
        help='Python file containing Kuyruk configuration parameters')
    parser.add_argument(
        '-m', '--module',
        help='Python module containing Kuyruk configuration parameters')
    add_config_options(parser)

    subparsers = parser.add_subparsers(help='sub-command name')

    # Parser for the "worker" sub-command
    parser_worker = subparsers.add_parser('worker', help='run a worker')
    parser_worker.set_defaults(func=run_worker)
    parser_worker.add_argument(
        '-q', '--queue', default='kuyruk', help='consume tasks from')
    parser_worker.add_argument(
        '-l', '--local', action="store_true",
        help='append hostname to the queue name')

    # Parse arguments
    args = parser.parse_args()

    # Run the sub-command function
    args.func(create_config(args), args)


def run_worker(config, args):
    worker_class = importer.import_class_str(config.WORKER_CLASS)
    w = worker_class(config, args)
    w.run()


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

    env_config = os.environ.get('KUYRUK_CONFIG')

    if args.module:
        config.from_pymodule(args.module)
    elif args.config:
        config.from_pyfile(args.config)
    elif env_config:
        assert os.path.isabs(env_config)
        config.from_pyfile(env_config)
    else:
        sys.stderr.write("------- No config given, using default config.\n")

    config.from_env_vars()
    config.from_cmd_args(args)
    return config


if __name__ == '__main__':
    main()
