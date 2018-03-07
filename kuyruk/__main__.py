"""
This is the entry point for main "kuyruk" executable command.
It implements the command line parsing for subcommands and configuration.

"""
import sys
import logging
import argparse
import pkg_resources

from kuyruk import __version__, importer, Kuyruk
from kuyruk.worker import Worker

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(conflict_handler='resolve')

    # Add common options
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('-a', '--app', required=True, help='path to the Kuyruk object')

    subparsers = parser.add_subparsers(dest='subparser_name', help='sub-command name')

    # Parser for the "worker" sub-command
    parser_worker = subparsers.add_parser('worker', help='run a worker')
    parser_worker.set_defaults(func=run_worker)
    parser_worker.add_argument(
        '-q',
        '--queue',
        dest='queues',
        default=[],
        action='append',
        help='consume tasks from queue (may be specified multiple times), '
        'if queue name ends with ".localhost" hostname will be appended '
        'to queue name')
    parser_worker.add_argument(
        '--max-load',
        type=float,
        help='pause consuming if load is above this value, '
        'set to 0 for disable, '
        'set to -1 for number of cpus on host')
    parser_worker.add_argument(
        '--max-run-time',
        type=int,
        help='gracefully shutdown worker after this duration in seconds, '
        'set to 0 for running forever')
    parser_worker.add_argument(
        '-l',
        '--logging-level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        help='console logging level')

    # Add additional subcommands from extensions.
    for entry_point in pkg_resources.iter_entry_points("kuyruk.commands"):
        command_func, help_text, customize_parser = entry_point.load()
        ext_parser = subparsers.add_parser(entry_point.name, help=help_text)
        ext_parser.set_defaults(func=command_func)
        if customize_parser:
            customize_parser(ext_parser)

    # Parse arguments
    args = parser.parse_args()

    # Import Kuyruk app
    sys.path.insert(0, '')
    app = importer.import_object_str(args.app)
    if not isinstance(app, Kuyruk):
        raise TypeError("%s is not an instance of kuyruk.Kuyruk" % args.app)

    # Run the sub-command function
    args.func(app, args)


def run_worker(app: Kuyruk, args: argparse.Namespace) -> None:
    w = Worker(app, args)
    w.run()


if __name__ == '__main__':
    main()
