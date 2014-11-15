from __future__ import absolute_import
import json
import logging

import rabbitpy
from setproctitle import setproctitle

from kuyruk.helpers import json_datetime
from kuyruk.process import KuyrukProcess

logger = logging.getLogger(__name__)


class Requeue(KuyrukProcess):

    def __init__(self, kuyruk):
        import redis
        super(Requeue, self).__init__(kuyruk)
        self.kuyruk = kuyruk
        self.redis = redis.StrictRedis(
            host=self.kuyruk.config.REDIS_HOST,
            port=self.kuyruk.config.REDIS_PORT,
            db=self.kuyruk.config.REDIS_DB,
            password=self.kuyruk.config.REDIS_PASSWORD)

    def run(self):
        super(Requeue, self).run()
        setproctitle("kuyruk: requeue")
        tasks = self.redis.hvals('failed_tasks')
        channel = self.kuyruk.channel()
        for task in tasks:
            task = json.loads(task)
            print "Requeueing task: %r" % task
            Requeue.requeue(task, channel, self.redis)

        print "%i failed tasks have been requeued." % len(tasks)
        logger.debug("End run requeue")

    @staticmethod
    def requeue(task_description, channel, redis):
        queue_name = task_description['queue']
        del task_description['queue']
        count = task_description.get('requeue_count', 0)
        task_description['requeue_count'] = count + 1
        body = json_datetime.dumps(task_description)
        msg = rabbitpy.Message(channel, body, properties={
            "delivery_mode": 2,
            "content_type": "application/json"})
        msg.publish("", routing_key=queue_name, mandatory=True)
        redis.hdel('failed_tasks', task_description['id'])
