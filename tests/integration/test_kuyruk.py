from tests.integration.util import new_instance


def test_new_connection():
    instance = new_instance()
    with instance.new_connection() as connection:
        channel = connection.channel()
        channel.close()
