from __future__ import absolute_import
import json
import logging

import redis

from kuyruk.channel import LazyChannel
from kuyruk.queue import Queue

logger = logging.getLogger(__name__)


class Requeuer(object):

    def __init__(self, config):
        self.config = config
        self.redis = redis.StrictRedis(
            host=self.config.REDIS_HOST,
            port=self.config.REDIS_PORT,
            db=self.config.REDIS_DB,
            password=self.config.REDIS_PASSWORD)

    def run(self):
        tasks = self.redis.hvals('failed_tasks')
        channel = LazyChannel.from_config(self.config)
        with channel:
            for task in tasks:
                task = json.loads(task)
                print "Requeueing task: %r" % task
                Requeuer.requeue(task, channel, self.redis)

        print "%i failed tasks have been requeued." % len(tasks)

    @staticmethod
    def requeue(task_description, channel, redis):
        queue_name = task_description['queue']
        del task_description['queue']
        task_queue = Queue(queue_name, channel)
        task_queue.send(task_description)
        redis.hdel('failed_tasks', task_description['id'])
