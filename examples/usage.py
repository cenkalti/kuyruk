import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('pika').level = logging.INFO

from kuyruk import Kuyruk
import config

kuyruk = Kuyruk(config)

@kuyruk.task
def echo(message):
    print message

# runs in background
echo('hello world')
