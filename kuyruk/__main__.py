"""
This is the entry point for main "kuyruk" executable command.
It implements the command line parsing for subcommands and configuration.

"""
from __future__ import absolute_import
import sys
import logging
import argparse
import pkg_resources

from kuyruk import __version__, importer, Kuyruk
from kuyruk.worker import Worker


logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(conflict_handler='resolve')

    # Add common options
    parser.add_argument(
        '-v', '--version', action='version', version=__version__)
    parser.add_argument(
        '-a', '--app', required=True, help='path to the Kuyruk object')

    subparsers = parser.add_subparsers(dest='subparser_name',
                                       help='sub-command name')

    # Parser for the "worker" sub-command
    parser_worker = subparsers.add_parser('worker', help='run a worker')
    parser_worker.set_defaults(func=run_worker)
    parser_worker.add_argument(
        '-q', '--queue', dest='queues', default=[], action='append',
        help='consume tasks from queue (may be specified multiple times)')
    parser_worker.add_argument(
        '-l', '--local', action="store_true",
        help='append hostname to the queue name')

    # Add additional subcommands from extensions.
    for entry_point in pkg_resources.iter_entry_points("kuyruk.commands"):
        command_func, help_text, customize_parser = entry_point.load()
        ext_parser = subparsers.add_parser(entry_point.name, help=help_text)
        ext_parser.set_defaults(func=command_func)
        if customize_parser:
            customize_parser(ext_parser)

    # Parse arguments
    args = parser.parse_args()

    # Use "kuyruk" if no queue is given
    if args.subparser_name == 'worker' and not args.queues:
        args.queues = ['kuyruk']

    # Import Kuyruk app
    sys.path.insert(0, '')
    app = importer.import_object_str(args.app)
    assert isinstance(app, Kuyruk)

    # Run the sub-command function
    args.func(app, args)


def run_worker(app, args):
    w = Worker(app, args)
    w.run()


if __name__ == '__main__':
    main()
