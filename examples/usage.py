#!/usr/bin/env python
import logging
logging.basicConfig(level=logging.INFO)

from tasks import echo

# Since echo method is wrapped with task decorator
# it is going to be send to queue to run in background.
echo('hello world')
