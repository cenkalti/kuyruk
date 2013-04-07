import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('pika').level = logging.INFO

from tasks import echo

echo('hello world')  # runs in background
