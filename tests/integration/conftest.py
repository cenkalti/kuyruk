import time

import amqp
import pytest

from kuyruk import Config


@pytest.fixture(scope="session", autouse=True)
def wait_for_rabbitmq(request):
    config = Config()
    config.from_pyfile('/tmp/kuyruk_config.py')

    while True:
        conn = amqp.Connection(
            host="%s:%s" % (config.RABBIT_HOST, config.RABBIT_PORT),
            userid=config.RABBIT_USER,
            password=config.RABBIT_PASSWORD,
            virtual_host=config.RABBIT_VIRTUAL_HOST,
            connect_timeout=1)

        try:
            conn.connect()
        except Exception:
            print("RabbitMQ is not ready yet.")
            time.sleep(1)
            continue

        conn.close()
        break
