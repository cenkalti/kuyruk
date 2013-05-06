from __future__ import absolute_import
import logging
import logging.config
import optparse

from kuyruk import __version__
from kuyruk.config import Config

logger = logging.getLogger(__name__)


class OptionParser(optparse.OptionParser):
    """Base class of all kuyruk executables.
    It adds some common options and does common operations when parsed.

    """
    LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

    def __init__(self):
        optparse.OptionParser.__init__(self, version=__version__)
        self.add_option(
            '-c', '--config',
            help='Python file containing Kuyruk configuration parameters')
        self.add_option(
            '-l', '--logging-level',
            default=self.LOG_LEVELS[1],
            choices=self.LOG_LEVELS,
            help="any of %s" % ', '.join(self.LOG_LEVELS))
        self.add_option(
            '--logging-config',
            help="INI style logging configuration file")

    def parse_args(self, args=None, values=None):
        options, args = optparse.OptionParser.parse_args(self)

        if options.logging_config:
            logging.config.fileConfig(options.logging_config)
        else:
            logging.getLogger('pika').level = logging.WARNING
            level = getattr(logging, options.logging_level)
            logging.basicConfig(level=level)

        self.config = Config()
        if options.config:
            self.config.from_pyfile(options.config)

        return options, args
