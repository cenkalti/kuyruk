import pytest

from tests.integration.util import override_connection_params

from testcontainers.rabbitmq import RabbitMqContainer


@pytest.fixture(scope="session", autouse=True)
def rabbitmq(request):
    rabbitmq = RabbitMqContainer("rabbitmq:3.12-management", username='kuyruk', password='123')
    rabbitmq.with_exposed_ports(15672)
    rabbitmq.start()
    override_connection_params(rabbitmq)

    yield rabbitmq

    rabbitmq.stop()
