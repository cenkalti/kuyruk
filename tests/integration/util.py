import os
import sys
import time
import errno
import logging
from time import sleep, monotonic
from functools import partial
from contextlib import contextmanager

import amqp
import psutil
import requests
from what import What

from kuyruk import Kuyruk, Config


TIMEOUT = 3
CONFIG_PATH = '/tmp/kuyruk_config.py'

logger = logging.getLogger(__name__)


instances = []


def new_instance():
    config = Config()
    if os.path.exists(CONFIG_PATH):
        config.from_pyfile(CONFIG_PATH)
    instance = Kuyruk(config=config)
    instances.append(instance)
    return instance


def override_connection_params(rabbitmq):
    params = rabbitmq.get_connection_params()

    # Writing a config for worker processes that we're going to start in this test suite.
    with open(CONFIG_PATH, 'w') as f:
        f.write(f"""
RABBIT_HOST = '{params.host}'
RABBIT_PORT = {params.port}
RABBIT_VIRTUAL_HOST = '{params.virtual_host}'
RABBIT_USER = 'kuyruk'
RABBIT_PASSWORD = '123'
""")

    for instance in instances:
        # Override default connection params.
        # Because the tasks module imported before rabbitmq server is created,
        # we have to manually override config values.
        # Kuyruk instance do not use the connection params until it sends a task.
        instance.config.RABBIT_HOST = params.host
        instance.config.RABBIT_PORT = params.port
        instance.config.RABBIT_VIRTUAL_HOST = params.virtual_host
        instance.config.RABBIT_USER = params.credentials.username
        instance.config.RABBIT_PASSWORD = params.credentials.password

        # Because the connection object is created inside the constructor
        # we have to manually override SingleConnection instance variables.
        instance._connection._host = params.host
        instance._connection._port = params.port
        instance._connection._vhost = params.virtual_host
        instance._connection._user = params.credentials.username
        instance._connection._password = params.credentials.password


@contextmanager
def amqp_channel(rabbitmq):
    params = rabbitmq.get_connection_params()
    connection = amqp.Connection(
        host="{h}:{p}".format(h=params.host, p=params.port),
        userid=params.credentials.username,
        password=params.credentials.password,
        virtual_host=params.virtual_host)
    try:
        connection.connect()
        channel = connection.channel()
        try:
            yield channel
        finally:
            channel.close()
    finally:
        connection.close()


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


def drop_connections(container, timeout):
    logger.debug("dropping connections...")
    params = container.get_connection_params()
    auth = (params.credentials.username, params.credentials.password)
    time.sleep(5)  # Need some time before the connections become available in management API.
    original_connections = _get_connections(container)

    def drop():
        _drop_connections(auth, original_connections)
        return not original_connections.intersection(_get_connections(container))

    wait_until(drop, timeout)

    for instance in instances:
        instance._connection.close(suppress_exceptions=True)


def _drop_connections(auth, connections):
    for url in connections:
        logger.debug("deleting connection: %s", url)
        r = requests.delete(url, auth=auth)
        if r.status_code not in [204, 404]:
            r.raise_for_status()


def _get_connections(container):
    logger.debug("getting connections...")
    params = container.get_connection_params()
    auth = (params.credentials.username, params.credentials.password)
    host = params.host
    port = container.get_exposed_port(15672)
    server = 'http://%s:%s' % (host, port)
    logger.info("connecting to %s", server)
    r = requests.get(server + '/api/connections', auth=auth)
    r.raise_for_status()
    ret = set()
    for conn in r.json():
        if conn['client_properties'].get('product') == 'py-amqp':
            logger.debug("matched connection: %s", conn['name'])
            name = conn['name']
            ret.add(server + '/api/connections/' + name)
    logger.debug("match count: %s", len(ret))
    return ret


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
