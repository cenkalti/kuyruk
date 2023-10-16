import os
import sys
import errno
import logging
from time import sleep, monotonic
from functools import partial
from contextlib import contextmanager

import psutil
import requests
from what import What

from kuyruk import Kuyruk, Config
from tests import tasks


TIMEOUT = 3

logger = logging.getLogger(__name__)


instances = []


def new_instance():
    config = Config()
    config.from_pyfile('/tmp/kuyruk_config.py')
    instance = Kuyruk(config=config)
    instances.append(instance)
    return instance


def delete_queue(*queues):
    """Delete queues from RabbitMQ"""
    with new_instance().connection() as conn:
        for name in queues:
            logger.debug("deleting queue %s", name)
            with conn.channel() as ch:
                ch.queue_delete(queue=name)


def len_queue(queue):
    with new_instance().channel() as ch:
        _, count, _ = ch.queue_declare(queue=queue, durable=True, passive=True, auto_delete=False)
        return count


def is_empty(queue):
    return len_queue(queue) == 0


def remove_connections():
    for instance in instances:
        instance._remove_connection()


def drop_connections(count, timeout):
    dropped_connections = set()

    def drop():
        for conn in _drop_connections():
            dropped_connections.add(conn)

        return len(dropped_connections) == count

    wait_until(drop, timeout)


def _drop_connections():
    logger.debug("dropping connections...")
    k = new_instance()
    host = k.config.RABBIT_HOST
    port = 15672
    server = 'http://%s:%s' % (host, port)
    auth = (k.config.RABBIT_USER, k.config.RABBIT_PASSWORD)
    r = requests.get(server + '/api/connections', auth=auth)
    r.raise_for_status()
    for conn in r.json():
        logger.debug("connection: %s", conn)
        print('conn: %r' % conn)
        if conn['client_properties'].get('product') == 'py-amqp':
            logger.debug("deleting connection: %s", conn['name'])
            name = conn['name']
            url = server + '/api/connections/' + name
            r = requests.delete(url, auth=auth)
            r.raise_for_status()
            yield name


@contextmanager
def run_worker(app='tests.tasks.kuyruk', terminate=True, **kwargs):
    assert not_running()
    args = [
        sys.executable, '-u',
        '-m', 'kuyruk.__main__',  # run main module
        '--app', app,
        'worker',
        '--logging-level', 'debug',
    ]
    for key, value in kwargs.items():
        args.extend(['--%s' % key.replace('_', '-'), str(value)])

    environ = os.environ.copy()
    environ['COVERAGE_PROCESS_START'] = '.coveragerc'

    popen = What(*args, preexec_fn=os.setsid, env=environ)
    popen.timeout = TIMEOUT
    try:
        yield popen

        if terminate:
            # Send SIGTERM to worker for gracefull shutdown
            popen.terminate()
            popen.expect("End run worker")

        popen.expect_exit()

    finally:
        # We need to make sure that not any process of kuyruk is running
        # after the test is finished.

        # Kill the process and wait until it is dead
        try:
            popen.kill()
            popen.wait()
        except OSError as e:
            if e.errno != errno.ESRCH:  # No such process
                raise

        logger.debug('Worker return code: %s', popen.returncode)

        try:
            wait_while(lambda: get_pids('kuyruk:'))
        except KeyboardInterrupt:
            print(popen.get_output())
            # Do not raise KeyboardInterrupt here because nose does not print
            # captured stdout and logging on KeyboardInterrupt
            raise Exception


def not_running():
    return not is_running()


def is_running():
    return bool(get_pids('kuyruk:'))


def get_pids(pattern):
    logger.debug('get_pids: %s', pattern)
    procs = []
    for p in psutil.process_iter():
        if p.name().startswith(pattern):
            procs.append(p)

    pids = [p.pid for p in procs]
    logger.debug('pids: %s', pids)
    return pids


def get_pid(pattern):
    pids = get_pids(pattern)
    assert len(pids) == 1
    return pids[0]


def wait_until(f, timeout=None):
    return wait_while(lambda: not f(), timeout)


def wait_while(f, timeout=None):
    do_while(partial(sleep, 0.1), f, timeout)


def do_until(f_do, f_cond, timeout=None):
    do_while(f_do, lambda: not f_cond(), timeout)


def do_while(f_do, f_condition, timeout=None):
    def should_do():
        if timeout and timeout < 0:
            raise Timeout
        return f_condition()

    original_timeout = timeout
    start = monotonic()
    while should_do():
        f_do()
        if timeout:
            passed = monotonic() - start
            timeout = original_timeout - passed


class Timeout(Exception):
    pass
