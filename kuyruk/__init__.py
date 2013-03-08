import sys
import imp
import logging
import optparse
import multiprocessing

import pika

from .task import Task
from .worker import Worker
from .queue import Queue

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

    def run(self, queue):
        rabbit_queue = Queue(queue, self.connection)
        in_queue = multiprocessing.Queue(1)
        out_queue = multiprocessing.Queue(1)
        worker = Worker(in_queue, out_queue)
        while 1:
            job = rabbit_queue.receive()
            if job is None:
                self.connection.sleep(1)
                continue

            in_queue.put(job)
            worker.work()
            delivery_tag, result = out_queue.get()
            logger.debug('Worker result: %r', result)
            if result == Worker.RESULT_OK:
                rabbit_queue.ack(delivery_tag)
            else:
                pass


def main():
    # from worker import Worker
    # from worker import create_job_handler

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('pika').level = logging.WARNING

    parser = optparse.OptionParser()
    parser.add_option('-c', '--config')
    # parser.add_option('-w', '--workers', type='int')
    # parser.add_option("-l", "--local",
    #                   action="store_true", default=False,
    #                   help="append hostname to queue name")
    # sleep on load
    # max load
    # max run time
    options, args = parser.parse_args()

    if not args:
        args = ['kuyruk']

    if options.config:
        config = imp.load_source('config', options.config)
    else:
        config = {}

    kuyruk = Kuyruk(config=config)
    kuyruk.run(args[0])

    # queue = model.lower() + '_' + method
    # module = __import__('putio.models', globals(), locals(), [model])
    # cls = getattr(module, model)
    # requirements_fn = getattr(cls, method + '_requirements', None)
    # if requirements_fn:
    #     requirements_fn()
    # fn = getattr(cls, method)
    # job_handler = create_job_handler(cls, fn)
    # Worker(queue, job_handler, local=options.local).run()
