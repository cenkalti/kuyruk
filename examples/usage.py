import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('pika').level = logging.INFO

from tasks import echo

# runs in background
echo('hello world')
