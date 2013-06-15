from __future__ import absolute_import
import json
import logging

from kuyruk.queue import Queue

logger = logging.getLogger(__name__)


class Requeuer(object):

    def __init__(self, kuyruk):
        import redis
        self.kuyruk = kuyruk
        self.redis = redis.StrictRedis(
            host=self.kuyruk.config.REDIS_HOST,
            port=self.kuyruk.config.REDIS_PORT,
            db=self.kuyruk.config.REDIS_DB,
            password=self.kuyruk.config.REDIS_PASSWORD)

    def run(self):
        tasks = self.redis.hvals('failed_tasks')
        channel = self.kuyruk.channel()
        for task in tasks:
            task = json.loads(task)
            print "Requeueing task: %r" % task
            Requeuer.requeue(task, channel, self.redis)

        print "%i failed tasks have been requeued." % len(tasks)

    @staticmethod
    def requeue(task_description, channel, redis):
        queue_name = task_description['queue']
        del task_description['queue']
        count = task_description.get('requeue_count', 0)
        task_description['requeue_count'] = count + 1
        task_queue = Queue(queue_name, channel)
        task_queue.send(task_description)
        redis.hdel('failed_tasks', task_description['id'])
