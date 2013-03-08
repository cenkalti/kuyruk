import sys
import logging
import optparse

import pika

from task import Task

logger = logging.getLogger(__name__)


class JobReject(Exception):
    pass


class Kuyruk(object):

    _connection = None

    def __init__(self, config={}):
        self.host = getattr(config, 'KUYRUK_RABBIT_HOST', 'localhost')
        self.port = getattr(config, 'KUYRUK_RABBIT_PORT', 5672)
        self.user = getattr(config, 'KUYRUK_RABBIT_USER', 'guest')
        self.password = getattr(config, 'KUYRUK_RABBIT_PASSWORD', 'guest')
        self.eager = getattr(config, 'KUYRUK_EAGER', False)

    @property
    def connected(self):
        return self._connection and self._connection.is_open

    def _connect(self):
        assert not self.connected
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host, port=self.port, credentials=credentials)
        self._connection = pika.BlockingConnection(parameters)
        logger.info('Connected to RabbitMQ')

    @property
    def connection(self):
        if not self.connected:
            self._connect()

        return self._connection

    def close(self):
        if self.connected:
            self.connection.close()
            logger.info('Connection closed')

    def task(self, f):
        return Task(f, self)


def main():
    from worker import Worker
    from worker import create_job_handler

    logging.basicConfig(level=logging.DEBUG)
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config')
    parser.add_option('-w', '--workers', type='int')
    parser.add_option("-l", "--local",
                      action="store_true", default=False,
                      help="append hostname to queue name")
    options, args = parser.parse_args()

    kuyruk = Kuyruk(
        num_workers=options.workers,
        config_path=options.config,
        local=options.local
    )

    sys.exit()

    queue = model.lower() + '_' + method
    module = __import__('putio.models', globals(), locals(), [model])
    cls = getattr(module, model)
    requirements_fn = getattr(cls, method + '_requirements', None)
    if requirements_fn:
        requirements_fn()
    fn = getattr(cls, method)
    job_handler = create_job_handler(cls, fn)
    Worker(queue, job_handler, local=options.local).run()
