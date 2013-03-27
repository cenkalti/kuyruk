import os
import time
import logging
import threading
import multiprocessing

from .task import Task
from .queue import Queue
from .worker import Worker
from .connection import LazyConnection

logger = logging.getLogger(__name__)


class Kuyruk(threading.Thread):

    def __init__(self, config={}):
        super(Kuyruk, self).__init__(target=self.run)

        self.host = getattr(config, 'KUYRUK_RABBIT_HOST', 'localhost')
        self.port = getattr(config, 'KUYRUK_RABBIT_PORT', 5672)
        self.user = getattr(config, 'KUYRUK_RABBIT_USER', 'guest')
        self.password = getattr(config, 'KUYRUK_RABBIT_PASSWORD', 'guest')
        self.eager = getattr(config, 'KUYRUK_EAGER', False)
        self.max_run_time = getattr(config, 'KUYRUK_MAX_RUN_TIME', None)
        self.max_tasks = getattr(config, 'KUYRUK_MAX_TASKS', None)
        self.max_load = getattr(config, 'KUYRUK_MAX_LOAD', None)
        self.local = getattr(config, 'KUYRUK_LOCAL', False)

        self._stop = threading.Event()
        self.num_tasks = 0

        self.connection = LazyConnection(
            self.host, self.port, self.user, self.password)

    def task(self, queue='kuyruk'):
        """Wrap functions with this decorator to convert them
        to background tasks."""
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

    def _max_run_time(self):
        if self.max_run_time is not None:
            passed_seconds = time.time() - self.started
            if passed_seconds >= self.max_run_time:
                logger.warning('Kuyruk run for %s seconds', passed_seconds)
                self.stop()

    def _max_tasks(self):
        if self.num_tasks == self.max_tasks:
            logger.warning('Kuyruk has processed %s tasks', self.max_tasks)
            self.stop()

    def _max_load(self):
        return os.getloadavg()[0] > self.max_load

    def run(self):
        if self.max_load is None:
            self.max_load = multiprocessing.cpu_count()

        channel = self.connection.channel()
        control_queue = multiprocessing.Queue()
        workers = []
        queue_names = ['kuyruk', 'kuyruk']  # temporary
        queues = {}
        for name in queue_names:
            queues[name] = Queue(name, channel)

        for i, item in enumerate(queues.iteritems()):
            name, queue = item
            parent_conn, child_conn = multiprocessing.Pipe()
            controller = object()
            controller.worker = Worker(i, child_conn, control_queue)
            controller.queue = queue
            controller.pipe = parent_conn
            workers.append(controller)

        for controller in workers:
            controller.worker.start()

        while True:
            worker_num, message = control_queue.get()
            worker = workers[worker_num]
            if message == 'get':
                task_description = worker.queue.receive()
                if task_description is None:
                    worker.pipe.send('sleep')
                else:
                    worker.pipe.send(task_description)
            elif message == 'result':



        workers = [(Worker(chiled_conn), q, parent_conn)]
        rabbit_queue = Queue('kuyruk', channel, self.local)
        in_queue = multiprocessing.Queue(1)
        out_queue = multiprocessing.Queue(1)
        worker = Worker(in_queue, out_queue)
        self.started = time.time()
        while self._runnable():
            if self._max_load():
                logger.debug('Load is over %s. Sleeping 10 seconds...')
                self.connection.sleep(10)
                continue

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
            self.num_tasks += 1

        channel.close()
        logger.warning('Kuyruk finished running')

    def _runnable(self):
        self._max_run_time()
        self._max_tasks()
        return not self._stop.isSet()

    def stop(self):
        logger.warning('Stopping kuyruk...')
        self._stop.set()
