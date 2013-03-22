import time
import logging
import multiprocessing

import pika

from .task import Task
from .worker import Worker
from .queue import Queue

logger = logging.getLogger(__name__)


class Kuyruk(object):

    _connection = None

    def __init__(self, config={}):
        self.host = getattr(config, 'KUYRUK_RABBIT_HOST', 'localhost')
        self.port = getattr(config, 'KUYRUK_RABBIT_PORT', 5672)
        self.user = getattr(config, 'KUYRUK_RABBIT_USER', 'guest')
        self.password = getattr(config, 'KUYRUK_RABBIT_PASSWORD', 'guest')
        self.eager = getattr(config, 'KUYRUK_EAGER', False)
        self.max_run_time = getattr(config, 'KUYRUK_MAX_RUN_TIME', None)
        self.max_tasks = getattr(config, 'KUYRUK_MAX_TASKS', None)

        self.exit = False
        self.num_tasks = 0

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

    def task(self, queue='kuyruk'):
        def decorator():
            def inner(f):
                queue_ = 'kuyruk' if callable(queue) else queue
                return Task(f, self, queue=queue_)

            return inner

        if callable(queue):
            logger.debug('task without args')
            return decorator()(queue)

        logger.debug('task with args')
        return decorator()

    def should_exit(self, start):
        if self.max_run_time:
            diff = time.time() - start
            if diff > self.max_run_time:
                logger.warning(
                    'Kuyruk run for %s seconds', self.max_run_time)
                return True

        if self.num_tasks >= self.max_tasks:
            logger.warning(
                'Kuyruk has processed %s tasks', self.max_tasks)
            return True

    def run(self, queue):
        rabbit_queue = Queue(queue, self.connection)
        in_queue = multiprocessing.Queue(1)
        out_queue = multiprocessing.Queue(1)
        worker = Worker(in_queue, out_queue)
        start = time.time()
        while not self.exit:
            if self.should_exit(start):
                logger.warning('Exiting...')
                break

            task_description = rabbit_queue.receive()
            if task_description is None:
                logger.debug('No tasks. Sleeping 1 second...')
                self.connection.sleep(1)
                continue

            in_queue.put(task_description)
            worker.work()
            delivery_tag, result = out_queue.get()
            logger.debug('Worker result: %r', result)
            actions = {
                Worker.RESULT_OK: rabbit_queue.ack,
                Worker.RESULT_ERROR: rabbit_queue.discard,
                Worker.RESULT_REJECT: rabbit_queue.reject
            }
            actions[result](delivery_tag)
