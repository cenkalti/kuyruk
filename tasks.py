import logging

from kuyruk import Kuyruk

logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(threadName)s %(message)s')

kuyruk = Kuyruk()


@kuyruk.task(retry=5, fail_delay=10000, reject_delay=10000)
def echo(message):
    logging.info("ECHO: %s", message)
    if message == 'raise an exception':
        raise Exception()
